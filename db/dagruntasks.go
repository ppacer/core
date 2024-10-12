// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"errors"
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
	Retry          int
	InsertTs       string
	Status         string
	StatusUpdateTs string
	Version        string
}

type DagRunTaskDetails struct {
	DagId          string
	TaskId         string
	PosDepth       int
	PosWidth       int
	ConfigJson     string
	TaskNotStarted bool
	ExecTs         string
	Retry          int
	InsertTs       string
	Status         string
	StatusUpdateTs string
	Version        string
}

// Reads DAG run tasks information from dagruntasks table for given DAG run.
func (c *Client) ReadDagRunTasks(ctx context.Context, dagId, execTs string) ([]DagRunTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag run tasks", "dagId", dagId, "execTs",
		execTs)
	dagruntasks := make([]DagRunTask, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunTasksQuery(), dagId,
		execTs)
	if qErr != nil {
		c.logger.Error("Failed querying dag run tasks", "dagId", dagId, "execTs",
			execTs, "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing dag run tasks", "dagId",
				dagId, "execTs", execTs, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagruntask, scanErr := parseDagRunTask(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a DagRunTask record", "dagId", dagId,
				"execTs", execTs, "err", scanErr)
			return nil, scanErr
		}
		dagruntasks = append(dagruntasks, dagruntask)
	}
	c.logger.Debug("Finished reading dag run tasks", "dagId", dagId, "execTs",
		execTs, "duration", time.Since(start))
	return dagruntasks, nil
}

// ReadDagRunTaskLatestRetries reads, for a given DAG run, latest retries of
// all tasks in that DAG run.
func (c *Client) ReadDagRunTaskLatestRetries(ctx context.Context, dagId, execTs string) ([]DagRunTask, error) {
	return readRowsContext(
		ctx, c.dbConn, c.logger, parseDagRunTask,
		c.readDagRunTaskLatestRetriesQuery(), dagId, execTs,
	)
}

// Reads all DAG tasks with all relevant details including node positions in
// DAG and task configurations.
func (c *Client) ReadDagRunTaskDetails(
	ctx context.Context, dagId, execTs string,
) ([]DagRunTaskDetails, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag run task details", "dagId", dagId,
		"execTs", execTs)
	drtDetails, err := readRowsContext(
		ctx, c.dbConn, c.logger, parseDagRunTaskDetails,
		c.readDagRunTaskDetailsQuery(), execTs, dagId,
	)
	if err == nil {
		c.logger.Debug("Finished reading dag run tasks", "dagId", dagId, "execTs",
			execTs, "duration", time.Since(start))
	}
	return drtDetails, err
}

// Read DAG run task with additional information including node positions and
// task configuration.
func (c *Client) ReadDagRunSingleTaskDetails(
	ctx context.Context, dagId, execTs, taskId string,
) (DagRunTaskDetails, error) {
	start := time.Now()
	c.logger.Debug("Start reading single dag run task details", "dagId", dagId,
		"execTs", execTs, "taskId", taskId)
	drtd, err := readRow(
		ctx, c.dbConn, c.logger, parseDagRunTaskDetails,
		c.readDagRunSingleTaskDetailsQuery(), execTs, dagId, taskId,
	)
	if err == nil {
		c.logger.Debug("Finished reading single dag run task details", "dagId",
			dagId, "execTs", execTs, "taskId", taskId, "duration",
			time.Since(start),
		)
	}
	return drtd, err
}

// Inserts new DagRunTask with default status SCHEDULED.
func (c *Client) InsertDagRunTask(ctx context.Context, dagId, execTs, taskId string, retry int, status string) error {
	start := time.Now()
	insertTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start inserting new dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "retry", retry)
	_, iErr := c.dbConn.ExecContext(
		ctx, c.insertDagRunTaskQuery(),
		dagId, execTs, taskId, retry, insertTs, status, insertTs,
		version.Version,
	)
	if iErr != nil {
		c.logger.Error("Failed to insert new dag run task", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "retry", retry,
			"status", status, "err", iErr)
		return iErr
	}
	c.logger.Debug("Finished inserting new dag run task", "dagId", dagId,
		"execTs", execTs, "taskId", taskId, "retry", retry, "duration",
		time.Since(start))
	return nil
}

// ReadDagRunTask reads information about given taskId in given dag run.
func (c *Client) ReadDagRunTask(ctx context.Context, dagId, execTs, taskId string, retry int) (DagRunTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading single dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "retry", retry)

	row := c.dbConn.QueryRowContext(ctx, c.readDagRunTaskQuery(), dagId,
		execTs, taskId, retry)
	var insertTs, status, statusTs, version string
	scanErr := row.Scan(&insertTs, &status, &statusTs, &version)
	if scanErr == sql.ErrNoRows {
		return DagRunTask{}, scanErr
	}
	if scanErr != nil {
		c.logger.Error("Failed scanning a DagRunTask record", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "err", scanErr)
		return DagRunTask{}, scanErr
	}

	dagRunTask := DagRunTask{
		DagId:          dagId,
		ExecTs:         execTs,
		TaskId:         taskId,
		Retry:          retry,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	c.logger.Debug("Finished reading dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "retry", retry, "duration",
		time.Since(start))
	return dagRunTask, nil
}

// ReadDagRunTaskLatest reads information about latest DAG run task try, out of
// possibly multiple retries of task execution, for given taskId and dag run.
func (c *Client) ReadDagRunTaskLatest(ctx context.Context, dagId, execTs, taskId string) (DagRunTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading latest dag run task", "dagId", dagId, "execTs",
		execTs, "taskId", taskId)

	row := c.dbConn.QueryRowContext(ctx, c.readDagRunTaskLatestQuery(), dagId,
		execTs, taskId)
	var insertTs, status, statusTs, version string
	var retry int
	scanErr := row.Scan(&retry, &insertTs, &status, &statusTs, &version)
	if scanErr == sql.ErrNoRows {
		return DagRunTask{}, scanErr
	}
	if scanErr != nil {
		c.logger.Error("Failed scanning a DagRunTask record", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "err", scanErr)
		return DagRunTask{}, scanErr
	}

	dagRunTask := DagRunTask{
		DagId:          dagId,
		ExecTs:         execTs,
		TaskId:         taskId,
		Retry:          retry,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	c.logger.Debug("Finished reading latest dag run task", "dagId", dagId,
		"execTs", execTs, "taskId", taskId, "duration", time.Since(start),
	)
	return dagRunTask, nil
}

// Updates dagruntask status for given dag run task.
func (c *Client) UpdateDagRunTaskStatus(ctx context.Context, dagId, execTs, taskId string, retry int, status string) error {
	start := time.Now()
	updateTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start updating dag run task status", "dagId", dagId, "execTs",
		execTs, "taskId", taskId, "status", status)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunTaskStatusQuery(),
		status, updateTs, dagId, execTs, taskId, retry,
	)
	if err != nil {
		c.logger.Error("Cannot update dag run task status", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "status", status, "err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		c.logger.Error("Seems that too many rows were updated. Expected exactly one",
			"dagId", dagId, "execTs", execTs, "taskId", taskId, "status",
			status, "rowsUpdated", rowsUpdated)
		return errors.New("too many rows updated")
	}
	c.logger.Debug("Finished updating dag run task status", "dagId", dagId,
		"execTs", execTs, "taskId", taskId, "status", status, "duration",
		time.Since(start))
	return nil
}

// ReadDagRunTasksNotFinished reads tasks from dagruntasks table which are not
// in terminal state (success or failed). Tasks are sorted from oldest to
// newest based on execTs and insertTs.
func (c *Client) ReadDagRunTasksNotFinished(ctx context.Context) ([]DagRunTask, error) {
	start := time.Now()
	c.logger.Debug("Start reading not finished dag run tasks")
	dagruntasks := make([]DagRunTask, 0, 10)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readNotFinishedDagRunTasksQuery(),
		statusFailed, statusSuccess)
	if qErr != nil {
		c.logger.Error("Failed querying not finished dag run tasks", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing dag run tasks", "err",
				ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagruntask, scanErr := parseDagRunTask(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a DagRunTask record", "err", scanErr)
			continue
		}
		dagruntasks = append(dagruntasks, dagruntask)
	}
	c.logger.Debug("Finished reading not finished dag run tasks", "duration",
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

// Reads aggregation of all DAG run tasks by its status.
func (c *Client) ReadDagRunTasksAggByStatus(ctx context.Context) (map[string]int, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag run tasks aggregation by status")
	result, err := groupBy2[string, int](
		ctx, c.dbConn, c.logger, c.readDagRunTasksAggByStatus(),
	)
	if err != nil {
		return result, err
	}
	c.logger.Debug("Finished reading dag run tasks aggregation by status",
		"duration", time.Since(start))
	return result, nil
}

func parseDagRunTask(rows Scannable) (DagRunTask, error) {
	var dagId, execTs, taskId, insertTs, status, statusTs, version string
	var retry int
	scanErr := rows.Scan(&dagId, &execTs, &taskId, &retry, &insertTs, &status,
		&statusTs, &version)
	if scanErr != nil {
		return DagRunTask{}, scanErr
	}
	dagRunTask := DagRunTask{
		DagId:          dagId,
		ExecTs:         execTs,
		TaskId:         taskId,
		Retry:          retry,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	return dagRunTask, nil
}

func parseDagRunTaskDetails(rows Scannable) (DagRunTaskDetails, error) {
	var (
		dagId, execTs, taskId, insertTs, status, statusTs, version,
		configJson string
	)
	var drtNotStarted, retry, posDepth, posWidth int
	scanErr := rows.Scan(
		&dagId, &taskId, &posDepth, &posWidth, &configJson,
		&drtNotStarted, &execTs, &retry, &insertTs, &status, &statusTs,
		&version,
	)
	if scanErr != nil {
		return DagRunTaskDetails{}, scanErr
	}
	drtDetails := DagRunTaskDetails{
		DagId:          dagId,
		TaskId:         taskId,
		PosDepth:       posDepth,
		PosWidth:       posWidth,
		ConfigJson:     configJson,
		TaskNotStarted: drtNotStarted == 1,
		ExecTs:         execTs,
		Retry:          retry,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	return drtDetails, nil
}

func (c *Client) readDagRunTasksQuery() string {
	return `
	SELECT
		DagId,
		ExecTs,
		TaskId,
		Retry,
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

func (c *Client) readDagRunTaskLatestRetriesQuery() string {
	return `
	WITH latest AS (
		SELECT
			DagId,
			ExecTs,
			TaskId,
			MAX(Retry) AS LatestRetry
		FROM
			dagruntasks
		WHERE
				DagId = ?
			AND ExecTs = ?
		GROUP BY
			DagId,
			ExecTs,
			TaskId
	)
	SELECT
		d.DagId,
		d.ExecTs,
		d.TaskId,
		d.Retry,
		d.InsertTs,
		d.Status,
		d.StatusUpdateTs,
		d.Version
	FROM
		dagruntasks d
	INNER JOIN
		latest l ON
			d.DagId = l.DagId
		AND d.ExecTs = l.ExecTs
		AND d.TaskId = l.TaskId
		AND d.Retry = l.LatestRetry
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
		AND Retry = ?
	`
}

func (c *Client) readDagRunTaskLatestQuery() string {
	return `
	SELECT
		Retry,
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
	ORDER BY
		Retry DESC
	LIMIT
		1
	`
}

func (c *Client) insertDagRunTaskQuery() string {
	return `
	INSERT INTO dagruntasks(
		DagId, ExecTs, TaskId, Retry, InsertTs, Status, StatusUpdateTs, Version
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
		AND Retry = ?
	`
}

func (c *Client) readNotFinishedDagRunTasksQuery() string {
	return `
	SELECT
		DagId,
		ExecTs,
		TaskId,
		Retry,
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

func (c *Client) readDagRunTasksAggByStatus() string {
	return `
	SELECT
		Status,
		COUNT(*) AS CNT
	FROM
		dagruntasks
	GROUP BY
		Status
`
}

func (c *Client) readDagRunTaskDetailsQuery() string {
	return `
	SELECT
		dt.DagId,
		dt.TaskId,
		dt.PosDepth,
		dt.PosWidth,
		dt.TaskConfig,
		drt.DagId IS NULL AS DagRunTaskNoStarted,
		IFNULL(drt.ExecTs, '') AS ExecTs,
		IFNULL(drt.Retry, -1) AS Retry,
		IFNULL(drt.InsertTs, '') AS InsertTs,
		IFNULL(drt.Status, '') AS Status,
		IFNULL(drt.StatusUpdateTs, '') AS StatusUpdateTs,
		IFNULL(drt.Version, '') AS Version
	FROM
		dagtasks dt
	LEFT JOIN
		dagruntasks drt ON
				dt.DagId = drt.DagId
			AND dt.TaskId = drt.TaskId
			AND drt.ExecTs = ?
	WHERE
			dt.DagId = ?
		AND dt.IsCurrent = 1
`
}

func (c *Client) readDagRunSingleTaskDetailsQuery() string {
	return `
	SELECT
		dt.DagId,
		dt.TaskId,
		dt.PosDepth,
		dt.PosWidth,
		dt.TaskConfig,
		drt.DagId IS NULL AS DagRunTaskNoStarted,
		IFNULL(drt.ExecTs, '') AS ExecTs,
		IFNULL(drt.Retry, -1) AS Retry,
		IFNULL(drt.InsertTs, '') AS InsertTs,
		IFNULL(drt.Status, '') AS Status,
		IFNULL(drt.StatusUpdateTs, '') AS StatusUpdateTs,
		IFNULL(drt.Version, '') AS Version
	FROM
		dagtasks dt
	LEFT JOIN
		dagruntasks drt ON
				dt.DagId = drt.DagId
			AND dt.TaskId = drt.TaskId
			AND drt.ExecTs = ?
	WHERE
			dt.DagId = ?
		AND dt.IsCurrent = 1
		AND dt.TaskId = ?
`
}
