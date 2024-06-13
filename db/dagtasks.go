// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/timeutils"
	"github.com/ppacer/core/version"
)

// DagTask represents single row in dagtasks table in the database.
type DagTask struct {
	DagId          string
	TaskId         string
	IsCurrent      bool
	InsertTs       string
	Version        string
	TaskTypeName   string
	TaskConfig     string
	TaskBodyHash   string
	TaskBodySource string
}

// InsertDagTasks inserts the tasks of given DAG to dagtasks table and set it
// as the current version. Previous versions would still be in dagtasks table
// but with set IsCurrent=0. In case of inserting any of dag's task insertion
// would be rollbacked (in terms of SQL transactions).
func (c *Client) InsertDagTasks(ctx context.Context, d dag.Dag) error {
	start := time.Now()
	insertTs := timeutils.ToString(time.Now())
	dagId := string(d.Id)
	c.logger.Debug("Start syncing dag and dagtasks tables", "dagId", dagId)
	tx, _ := c.dbConn.Begin()

	// Make IsCurrent=0 for outdated rows
	uErr := c.outdateDagTasks(ctx, tx, dagId)
	if uErr != nil {
		c.logger.Error("Cannot outdate old dagtasks", "dagId", dagId, "err",
			uErr)
		return uErr
	}

	for _, node := range d.FlattenNodes() {
		iErr := c.insertSingleDagTask(ctx, tx, dagId, node.Node, insertTs)
		if iErr != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				c.logger.Error("Error while rollbacking SQL transaction", "err",
					rollErr)
			}
			return errors.New("could not sync dag and dagtasks properly. SQL transaction was rollbacked")
		}
	}

	cErr := tx.Commit()
	if cErr != nil {
		c.logger.Error("Could not commit SQL transaction", "dagId", dagId, "err",
			cErr)
		return cErr
	}
	c.logger.Debug("Finished syncing dag and dagtasks tables", "dagId", dagId,
		"duration", time.Since(start))
	return nil
}

// insertSingleDagTask inserts new DagTask which represents a task within a
// DAG. Using multiple InsertDagTask for dagId and task is a common case.
// Newely inserted version will have IsCurrent=1 and others will not. On
// database side (DagId, TaskId, IsCurrent) defines primary key on dagtasks
// table.
func (c *Client) insertSingleDagTask(
	ctx context.Context, tx *sql.Tx, dagId string, node *dag.Node,
	insertTs string,
) error {
	start := time.Now()
	c.logger.Debug("Start inserting new dag task", "dagId", dagId, "taskId",
		node.Task.Id(), "insertTs", insertTs)

	if node == nil {
		return fmt.Errorf("cannot insert nil dag.Node for dagId=%s, insertTs=%s",
			dagId, insertTs)
	}

	// Insert dagtask row
	iErr := c.insertDagTask(ctx, tx, dagId, node, insertTs)
	if iErr != nil {
		c.logger.Error("Cannot insert new dagtask", "dagId", dagId, "taskId",
			node.Task.Id(), "err", iErr)
		return iErr
	}
	c.logger.Debug("Finished inserting new DagTask", "dagId", dagId, "taskId",
		node.Task.Id(), "duration", time.Since(start))
	return nil
}

// Insert new row in dagtasks table.
func (c *Client) insertDagTask(
	ctx context.Context, tx *sql.Tx, dagId string, node *dag.Node,
	insertTs string,
) error {
	tTypeName := reflect.TypeOf(node.Task).Name()
	taskBody := dag.TaskExecuteSource(node.Task)
	taskHash := dag.TaskHash(node.Task)

	taskConfigJson, jErr := json.Marshal(node.Config)
	if jErr != nil {
		return fmt.Errorf("cannot serialize Task %s config into JSON: %w",
			node.Task.Id(), jErr)
	}

	_, err := tx.ExecContext(
		ctx,
		c.dagTaskInsertQuery(),
		dagId, node.Task.Id(), 1, insertTs, version.Version, tTypeName,
		string(taskConfigJson), taskHash, taskBody,
	)
	if err != nil {
		return err
	}
	return nil
}

// Outdates all rows in dagtasks table for given dagId.
func (c *Client) outdateDagTasks(
	ctx context.Context, tx *sql.Tx, dagId string,
) error {
	_, err := tx.ExecContext(ctx, c.dagTaskOutdateQuery(), dagId)
	if err != nil {
		return err
	}
	return nil
}

// ReadDagTasks reads all tasks for given dagId in the current version from
// dagtasks table.
func (c *Client) ReadDagTasks(ctx context.Context, dagId string) ([]DagTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading DagTasks", "dagId", dagId)

	rows, queryErr := c.dbConn.QueryContext(ctx, c.readDagTasksQuery(), dagId)
	if queryErr != nil {
		c.logger.Error("Failed querying DagTasks", "dagId", dagId, "err",
			queryErr)
		return nil, queryErr
	}
	defer rows.Close()
	var tasks []DagTask

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing DagTask rows", "dagId",
				dagId, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		task, scanErr := c.scanAndParseDagTask(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a DagTask record", "dagId", dagId,
				"err", scanErr)
			return nil, scanErr
		}
		tasks = append(tasks, task)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		c.logger.Error("Failed interating over DagTask rows", "dagId", dagId,
			"err", rowsErr)
		return nil, rowsErr
	}
	c.logger.Debug("Finished reading DagTasks", "dagId", dagId, "duration",
		time.Since(start))
	return tasks, nil
}

// ReadDagTask reads single row (current version) from dagtasks table for given
// DAG ID and task ID.
func (c *Client) ReadDagTask(ctx context.Context, dagId, taskId string) (DagTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading DagTask", "dagId", dagId, "taskId", taskId)

	row := c.dbConn.QueryRowContext(ctx, c.readDagTaskQuery(), dagId, taskId)
	dagtask, err := c.scanAndParseDagTask(row)
	if err != nil {
		return dagtask, err
	}
	c.logger.Debug("Finished reading DagTask", "dagId", dagId, "taskId", taskId,
		"duration", time.Since(start))
	return dagtask, nil
}

// scanAndParseDagTask try to parse given SQL row as DagTask.
func (c *Client) scanAndParseDagTask(s interface{ Scan(dest ...any) error }) (DagTask, error) {
	var (
		dId, tId, typeName, insertTs, version, config, bodyHash,
		bodySource string
	)
	var isCurrent int
	scanErr := s.Scan(&dId, &tId, &isCurrent, &insertTs, &version, &typeName,
		&config, &bodyHash, &bodySource)
	if scanErr == sql.ErrNoRows {
		return DagTask{}, scanErr
	}

	if scanErr != nil {
		c.logger.Error("Failed scanning DagTask record", "err", scanErr)
		return DagTask{}, scanErr
	}

	dagtask := DagTask{
		DagId:          dId,
		TaskId:         tId,
		IsCurrent:      isCurrent == 1,
		Version:        version,
		TaskTypeName:   typeName,
		TaskConfig:     config,
		TaskBodyHash:   bodyHash,
		TaskBodySource: bodySource,
	}
	return dagtask, nil
}

func (c *Client) readDagTaskQuery() string {
	return `
		SELECT
			DagId,
			TaskId,
			IsCurrent,
			InsertTs,
			Version,
			TaskTypeName,
			TaskConfig,
			TaskBodyHash,
			TaskBodySource
		FROM
			dagtasks
		WHERE
				dagId = ?
			AND taskId = ?
			AND IsCurrent = 1
	`
}

func (c *Client) readDagTasksQuery() string {
	return `
		SELECT
			DagId,
			TaskId,
			IsCurrent,
			InsertTs,
			Version,
			TaskTypeName,
			TaskConfig,
			TaskBodyHash,
			TaskBodySource
		FROM
			dagtasks
		WHERE
				dagId = ?
			--AND IsCurrent = 1
		ORDER BY
			DagId,
			IsCurrent,
			InsertTs,
			TaskId
	`
}

func (c *Client) dagTaskInsertQuery() string {
	return `
		INSERT INTO dagtasks (
			DagId, TaskId, IsCurrent, InsertTs, Version, TaskTypeName,
			TaskConfig, TaskBodyHash, TaskBodySource
		)
		VALUES (?,?,?,?,?,?,?,?,?)
	`
}

func (c *Client) dagTaskOutdateQuery() string {
	return `
		UPDATE
			dagtasks
		SET
			IsCurrent = 0
		WHERE
			DagId = ?
	`
}
