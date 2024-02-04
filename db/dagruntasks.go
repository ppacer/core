// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/ppacer/core/timeutils"
	"github.com/ppacer/core/version"
)

const (
	DagRunTaskStatusScheduled = "SCHEDULED"
)

type DagRunTask struct {
	DagId          string
	ExecTs         string
	TaskId         string
	InsertTs       string
	Status         string
	StatusUpdateTs string
	Version        string
}

// Reads DAG run tasks information from dagruntasks table for given DAG run.
func (c *Client) ReadDagRunTasks(ctx context.Context, dagId, execTs string) ([]DagRunTask, error) {
	start := time.Now()
	slog.Debug("Start reading dag run tasks", "dagId", dagId, "execTs", execTs)
	dagruntasks := make([]DagRunTask, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunTasksQuery(), dagId,
		execTs)
	if qErr != nil {
		slog.Error("Failed querying dag run tasks", "dagId", dagId, "execTs",
			execTs, "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			slog.Warn("Context done while processing dag run tasks", "dagId",
				dagId, "execTs", execTs, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagruntask, scanErr := parseDagRunTask(rows)
		if scanErr != nil {
			slog.Error("Failed scanning a DagRunTask record", "dagId", dagId,
				"execTs", execTs, "err", scanErr)
			return nil, scanErr
		}
		dagruntasks = append(dagruntasks, dagruntask)
	}
	slog.Debug("Finished reading dag run tasks", "dagId", dagId, "execTs",
		execTs, "duration", time.Since(start))
	return dagruntasks, nil
}

// Inserts new DagRunTask with default status SCHEDULED.
func (c *Client) InsertDagRunTask(ctx context.Context, dagId, execTs, taskId, status string) error {
	start := time.Now()
	insertTs := timeutils.ToString(start)
	slog.Debug("Start inserting new dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId)
	_, iErr := c.dbConn.ExecContext(
		ctx, c.insertDagRunTaskQuery(),
		dagId, execTs, taskId, insertTs, status, insertTs,
		version.Version,
	)
	if iErr != nil {
		slog.Error("Failed to insert new dag run task", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "err", iErr)
	}
	slog.Debug("Finished inserting new dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "duration", time.Since(start))
	return nil
}

// ReadDagRunTask reads information about given taskId in given dag run.
func (c *Client) ReadDagRunTask(ctx context.Context, dagId, execTs, taskId string) (DagRunTask, error) {
	start := time.Now()
	slog.Debug("Start reading single dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId)

	row := c.dbConn.QueryRowContext(ctx, c.readDagRunTaskQuery(), dagId,
		execTs, taskId)
	var insertTs, status, statusTs, version string
	scanErr := row.Scan(&insertTs, &status, &statusTs, &version)
	if scanErr == sql.ErrNoRows {
		return DagRunTask{}, scanErr
	}
	if scanErr != nil {
		slog.Error("Failed scanning a DagRunTask record", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "err", scanErr)
		return DagRunTask{}, scanErr
	}

	dagRunTask := DagRunTask{
		DagId:          dagId,
		ExecTs:         execTs,
		TaskId:         taskId,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	slog.Debug("Finished reading dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "duration", time.Since(start))
	return dagRunTask, nil
}

// Updates dagruntask status for given dag run task.
func (c *Client) UpdateDagRunTaskStatus(ctx context.Context, dagId, execTs, taskId, status string) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	slog.Debug("Start updating dag run task status", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "status", status)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunTaskStatusQuery(),
		status, updateTs, dagId, execTs, taskId,
	)
	if err != nil {
		slog.Error("Cannot update dag run task status", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "status", status, "err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		slog.Error("Seems that too many rows were updated. Expected exactly one",
			"dagId", dagId, "execTs", execTs, "taskId", taskId, "status",
			status, "rowsUpdated", rowsUpdated)
		return errors.New("too many rows updated")
	}
	slog.Debug("Finished updating dag run task status", "dagId", dagId,
		"execTs", execTs, "taskId", taskId, "status", status, "duration",
		time.Since(start))
	return nil
}

// ReadDagRunTasksNotFinished reads tasks from dagruntasks table which are not
// in terminal state (success or failed). Tasks are sorted from oldest to
// newest based on execTs and insertTs.
func (c *Client) ReadDagRunTasksNotFinished(ctx context.Context) ([]DagRunTask, error) {
	start := time.Now()
	slog.Debug("Start reading not finished dag run tasks")
	dagruntasks := make([]DagRunTask, 0, 10)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readNotFinishedDagRunTasksQuery(),
		statusFailed, statusSuccess)
	if qErr != nil {
		slog.Error("Failed querying not finished dag run tasks", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			slog.Warn("Context done while processing dag run tasks", "dagId",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagruntask, scanErr := parseDagRunTask(rows)
		if scanErr != nil {
			slog.Error("Failed scanning a DagRunTask record", "err", scanErr)
			continue
		}
		dagruntasks = append(dagruntasks, dagruntask)
	}
	slog.Debug("Finished reading not finished dag run tasks", "duration",
		time.Since(start))
	return dagruntasks, nil
}

// RunningTasksNum returns number of currently running tasks. That means rows
// in dagruntasks table with status 'RUNNING'.
func (c *Client) RunningTasksNum(ctx context.Context) (int, error) {
	q := "SELECT COUNT(*) FROM dagruntasks WHERE Status=?"
	rows, err := c.singleInt64(ctx, q, statusRunning)
	if err != nil {
		return -1, err
	}
	return int(rows), nil
}

func parseDagRunTask(rows *sql.Rows) (DagRunTask, error) {
	var dagId, execTs, taskId, insertTs, status, statusTs, version string
	scanErr := rows.Scan(&dagId, &execTs, &taskId, &insertTs, &status,
		&statusTs, &version)
	if scanErr != nil {
		return DagRunTask{}, scanErr
	}
	dagRunTask := DagRunTask{
		DagId:          dagId,
		ExecTs:         execTs,
		TaskId:         taskId,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	return dagRunTask, nil
}

func (c *Client) readDagRunTasksQuery() string {
	return `
	SELECT
		DagId,
		ExecTs,
		TaskId,
		InsertTs,
		Status,
		StatusUpdateTs,
		Version
	FROM
		dagruntasks
	WHERE
			DagId = ?
		AND ExecTs = ?
	`
}

func (c *Client) readDagRunTaskQuery() string {
	return `
	SELECT
		InsertTs,
		Status,
		StatusUpdateTs,
		Version
	FROM
		dagruntasks
	WHERE
			DagId = ?
		AND ExecTs = ?
		AND TaskId = ?
	`
}

func (c *Client) insertDagRunTaskQuery() string {
	return `
	INSERT INTO dagruntasks(
		DagId, ExecTs, TaskId, InsertTs, Status, StatusUpdateTs, Version
	)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	`
}

func (c *Client) updateDagRunTaskStatusQuery() string {
	return `
	UPDATE
		dagruntasks
	SET
		Status = ?,
		StatusUpdateTs = ?
	WHERE
			DagId = ?
		AND ExecTs = ?
		AND TaskId = ?
	`
}

func (c *Client) readNotFinishedDagRunTasksQuery() string {
	return `
	SELECT
		DagId,
		ExecTs,
		TaskId,
		InsertTs,
		Status,
		StatusUpdateTs,
		Version
	FROM
		dagruntasks
	WHERE
		Status NOT IN (?, ?)
	ORDER BY
		ExecTs ASC,
		InsertTs ASC
	`
}
