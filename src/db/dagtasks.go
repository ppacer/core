package db

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/timeutils"
	"github.com/dskrzypiec/scheduler/src/version"
)

// DagTask represents single row in dagtasks table in the database.
type DagTask struct {
	DagId          string
	TaskId         string
	IsCurrent      bool
	InsertTs       string
	Version        string
	TaskTypeName   string
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
	slog.Debug("Start syncing dag and dagtasks tables", "dagId", dagId)
	tx, _ := c.dbConn.Begin()

	// Make IsCurrent=0 for outdated rows
	uErr := c.outdateDagTasks(ctx, tx, dagId)
	if uErr != nil {
		slog.Error("Cannot outdate old dagtasks", "dagId", dagId, "err", uErr)
		return uErr
	}

	for _, task := range d.Flatten() {
		iErr := c.insertSingleDagTask(ctx, tx, dagId, task, insertTs)
		if iErr != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				slog.Error("Error while rollbacking SQL transaction", "err",
					rollErr)
			}
			return errors.New("could not sync dag and dagtasks properly. SQL transaction was rollbacked")
		}
	}

	cErr := tx.Commit()
	if cErr != nil {
		slog.Error("Could not commit SQL transaction", "dagId", dagId, "err",
			cErr)
		return cErr
	}
	slog.Debug("Finished syncing dag and dagtasks tables", "dagId", dagId,
		"duration", time.Since(start))
	return nil
}

// insertSingleDagTask inserts new DagTask which represents a task within a
// DAG. Using multiple InsertDagTask for dagId and task is a common case.
// Newely inserted version will have IsCurrent=1 and others will not. On
// database side (DagId, TaskId, IsCurrent) defines primary key on dagtasks
// table.
func (c *Client) insertSingleDagTask(
	ctx context.Context, tx *sql.Tx, dagId string, task dag.Task,
	insertTs string,
) error {
	start := time.Now()
	slog.Debug("Start inserting new dag task", "dagId", dagId, "taskId",
		task.Id(), "insertTs", insertTs)

	// Insert dagtask row
	iErr := c.insertDagTask(ctx, tx, dagId, task, insertTs)
	if iErr != nil {
		slog.Error("Cannot insert new dagtask", "dagId", dagId, "taskId",
			task.Id(), "err", iErr)
		return iErr
	}
	slog.Debug("Finished inserting new DagTask", "dagId", dagId, "taskId",
		task.Id(), "duration", time.Since(start))
	return nil
}

// Insert new row in dagtasks table.
func (c *Client) insertDagTask(
	ctx context.Context, tx *sql.Tx, dagId string, task dag.Task,
	insertTs string,
) error {
	tTypeName := reflect.TypeOf(task).Name()
	taskBody := dag.TaskExecuteSource(task)
	taskHash := dag.TaskHash(task)

	_, err := tx.ExecContext(
		ctx,
		c.dagTaskInsertQuery(),
		dagId, task.Id(), 1, insertTs, version.Version, tTypeName, taskHash,
		taskBody,
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
	slog.Debug("Start reading DagTasks", "dagId", dagId)

	rows, queryErr := c.dbConn.QueryContext(ctx, c.readDagTasksQuery(), dagId)
	if queryErr != nil {
		slog.Error("Failed querying DagTasks", "dagId", dagId, "err", queryErr)
		return nil, queryErr
	}
	defer rows.Close()
	var tasks []DagTask

	for rows.Next() {
		select {
		case <-ctx.Done():
			slog.Warn("Context done while processing DagTask rows", "dagId",
				dagId, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		var fetchedDagId, fetchedTaskId, typeName, insertTs, version, bodyHash,
			bodySource string
		var isCurrentInt int

		scanErr := rows.Scan(&fetchedDagId, &fetchedTaskId, &isCurrentInt,
			&insertTs, &version, &typeName, &bodyHash, &bodySource)
		if scanErr != nil {
			slog.Error("Failed scanning a DagTask record", "dagId", dagId,
				"err", scanErr)
			return nil, scanErr
		}
		task := DagTask{
			DagId:          fetchedDagId,
			TaskId:         fetchedTaskId,
			IsCurrent:      isCurrentInt == 1,
			InsertTs:       insertTs,
			Version:        version,
			TaskTypeName:   typeName,
			TaskBodyHash:   bodyHash,
			TaskBodySource: bodySource,
		}
		tasks = append(tasks, task)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		slog.Error("Failed interating over DagTask rows", "dagId", dagId, "err",
			rowsErr)
		return nil, rowsErr
	}
	slog.Debug("Finished reading DagTasks", "dagId", dagId, "duration",
		time.Since(start))
	return tasks, nil
}

// ReadDagTask reads single row (current version) from dagtasks table for given
// DAG ID and task ID.
func (c *Client) ReadDagTask(ctx context.Context, dagId, taskId string) (DagTask, error) {
	start := time.Now()
	slog.Debug("Start reading DagTask", "dagId", dagId, "taskId", taskId)

	row := c.dbConn.QueryRowContext(ctx, c.readDagTaskQuery(), dagId, taskId)
	var dId, tId, typeName, insertTs, version, bodyHash, bodySource string
	var isCurrent int
	scanErr := row.Scan(&dId, &tId, &isCurrent, &insertTs, &version, &typeName,
		&bodyHash, &bodySource)
	if scanErr == sql.ErrNoRows {
		return DagTask{}, scanErr
	}

	if scanErr != nil {
		slog.Error("Failed scanning DagTask record", "dagId", dagId, "taskId",
			taskId)
		return DagTask{}, scanErr
	}

	dagtask := DagTask{
		DagId:          dId,
		TaskId:         tId,
		IsCurrent:      isCurrent == 1,
		Version:        version,
		TaskTypeName:   typeName,
		TaskBodyHash:   bodyHash,
		TaskBodySource: bodySource,
	}
	slog.Debug("Finished reading DagTask", "dagId", dagId, "taskId", taskId,
		"duration", time.Since(start))
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
			TaskBodyHash, TaskBodySource
		)
		VALUES (?,?,?,?,?,?,?,?)
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
