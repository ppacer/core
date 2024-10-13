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

// DagRun represent a row of data in dagruns table.
type DagRun struct {
	RunId          int64
	DagId          string
	ExecTs         string
	InsertTs       string
	Status         string
	StatusUpdateTs string
	Version        string
}

// DagRunWithTaskInfo contains information about a DAG run and additionally
// information about that DAG tasks.
type DagRunWithTaskInfo struct {
	DagRun           DagRun
	TaskNum          int
	TaskCompletedNum int
}

// Those should be consistent with dag.RunStatus string values. We cannot use
// those in here, because db package cannot depend on dag package.
const (
	statusReadyToSchedule = "READY_TO_SCHEDULE"
	statusScheduled       = "SCHEDULED"
	statusSuccess         = "SUCCESS"
	statusFailed          = "FAILED"
	statusRunning         = "RUNNING"
)

// ReadDagRuns reads topN latest dag runs for given DAG ID.
func (c *Client) ReadDagRuns(ctx context.Context, dagId string, topN int) ([]DagRun, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag runs from DB", "dagId", dagId, "topN", topN)
	cap := topN
	if topN < 0 {
		cap = 1000
	}
	dagruns := make([]DagRun, 0, cap)
	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunsQuery(topN), dagId, topN)
	if qErr != nil {
		c.logger.Error("Failed querying dag runs", "dagId", dagId, "topN", topN,
			"err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			c.logger.Warn("Context done while processing rows", "dagId", dagId,
				"topN", topN, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning dagrun record", "dagId", dagId, "err",
				scanErr)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}
	c.logger.Debug("Finished reading dag runs", "dagId", dagId, "topN", topN,
		"duration", time.Since(start))
	return dagruns, nil
}

// InsertDagRun inserts new row into dagruns table for given DagId and
// execution timestamp. Initial status is set to DagRunStatusScheduled. RunId
// for just inserted dag run is returned or -1 in case when error is not nil.
func (c *Client) InsertDagRun(ctx context.Context, dagId, execTs string) (int64, error) {
	start := time.Now()
	insertTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start inserting dag run", "dagId", dagId, "execTs", insertTs)
	res, err := c.dbConn.ExecContext(
		ctx, c.insertDagRunQuery(),
		dagId, execTs, insertTs, statusScheduled, insertTs,
		version.Version,
	)
	if err != nil {
		c.logger.Error("Cannot insert new dag run", "dagId", dagId, "execTs",
			execTs, "err", err)
		return -1, err
	}
	c.logger.Debug("Finished inserting dag run in state SCHEDULED", "dagId",
		dagId, "execTs", execTs, "duration", time.Since(start))
	return res.LastInsertId()
}

// ReadLatestDagRuns reads latest dag run for each Dag. Returns map from DagId
// to DagRun.
func (c *Client) ReadLatestDagRuns(ctx context.Context) (map[string]DagRun, error) {
	start := time.Now()
	c.logger.Debug("Start reading latest run for each DAG from dagruns table")
	dagruns := make(map[string]DagRun, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.latestDagRunsQuery())
	if qErr != nil {
		c.logger.Error("Failed querying latest DAG runs", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			c.logger.Warn("Context done while processing latest dag run rows",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}

		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning dagrun record", "err", scanErr)
			return nil, scanErr
		}
		dagruns[dagrun.DagId] = dagrun
	}
	c.logger.Debug("Finished reading latest dag runs", "duration",
		time.Since(start))
	return dagruns, nil
}

// Reads DAG run information for given run ID.
func (c *Client) ReadDagRun(ctx context.Context, runId int) (DagRun, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag run with task info", "runId", runId)

	row := c.dbConn.QueryRowContext(ctx, c.readDagRunWithTaskInfoQuery(), runId)
	drti, scanErr := parseDagRun(row)
	if scanErr != nil {
		c.logger.Error("Failed scanning a dagrun with task info record", "err", scanErr)
		return DagRun{}, scanErr
	}
	c.logger.Debug("Finished reading dag runs with task info", "runId", runId,
		"duration", time.Since(start))
	return drti, nil
}

// ReadDagRunByExecTs reads DAG run information for given DAG ID and execTs.
func (c *Client) ReadDagRunByExecTs(ctx context.Context, dagId, execTs string) (DagRun, error) {
	return readRow(
		ctx, c.dbConn, c.logger, parseDagRun, c.readDagRunByExecTsQuery(),
		dagId, execTs,
	)
}

// Updates dagrun status for given runId.
func (c *Client) UpdateDagRunStatus(
	ctx context.Context, runId int64, status string,
) error {
	start := time.Now()
	updateTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start updating dag run status", "runId", runId)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunStatusQuery(), status, updateTs, runId,
	)
	if err != nil {
		c.logger.Error("Cannot update dag run", "runId", runId, "status",
			status, "err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		c.logger.Error("Seems that too many rows were updated. Expected exactly one",
			"runId", runId, "rowsUpdated", rowsUpdated)
		return errors.New("too many rows updated")
	}
	c.logger.Debug("Finished updating dag run", "runId", runId, "status",
		status, "duration", time.Since(start))
	return nil
}

// Updates dagrun status for given dagId and execTs (when runId is not
// available). Pair (dagId, execTs) is unique in dagrun table.
func (c *Client) UpdateDagRunStatusByExecTs(
	ctx context.Context, dagId, execTs, status string,
) error {
	start := time.Now()
	updateTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start updating dag run status by execTs", "dagId", dagId,
		"execTs", execTs, "status", status)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunStatusByExecTsQuery(),
		status, updateTs, dagId, execTs,
	)
	if err != nil {
		c.logger.Error("Cannot update dag run", "dagId", dagId, "execTs",
			execTs, "status", status, "err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		c.logger.Error("Seems that too many rows were updated. Expected exactly one",
			"dagId", dagId, "execTs", execTs, "status", status, "rowsUpdated",
			rowsUpdated)
		return errors.New("too many rows updated")
	}
	c.logger.Debug("Finished updating dag run", "dagId", dagId, "execTs", execTs,
		"status", status, "duration", time.Since(start))
	return nil
}

// DagRunAlreadyScheduled checks whenever dagrun already exists for given DAG ID and
// schedule timestamp.
func (c *Client) DagRunAlreadyScheduled(
	ctx context.Context, dagId, execTs string,
) (bool, error) {
	start := time.Now()
	c.logger.Debug("Start DagRunExists query", "dagId", dagId, "execTs", execTs)

	q := "SELECT COUNT(*) FROM dagruns WHERE DagId=? AND ExecTs=? AND (Status=? OR Status=?)"
	row := c.dbConn.QueryRowContext(
		ctx, q, dagId, execTs, statusScheduled, statusReadyToSchedule,
	)
	var count int
	err := row.Scan(&count)
	if err != nil {
		c.logger.Error("Cannot execute DagRunExists query", "dagId", dagId,
			"execTs", execTs, "err", err)
		return false, err
	}
	c.logger.Debug("Finished DagRunExists query", "dagId", dagId, "execTs",
		execTs, "duration", time.Since(start))
	return count > 0, nil
}

// Reads dag run from dagruns table which are not in terminal states.
func (c *Client) ReadDagRunsNotFinished(ctx context.Context) ([]DagRun, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag runs that should be scheduled")
	dagruns := make([]DagRun, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunNotFinishedQuery(),
		statusSuccess, statusFailed)
	if qErr != nil {
		c.logger.Error("Failed querying dag run to be scheduled", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing dag runs to be scheduled",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a dagrun record", "err", scanErr)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}
	c.logger.Debug("Finished reading dag runs to be scheduled", "duration",
		time.Since(start))
	return dagruns, nil
}

// Reads aggregation of all DAG run by its status.
func (c *Client) ReadDagRunsAggByStatus(ctx context.Context) (map[string]int, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag runs aggregation by status")
	result, err := groupBy2[string, int](
		ctx, c.dbConn, c.logger, c.readDagRunsAggByStatus(),
	)
	if err != nil {
		return result, err
	}
	c.logger.Debug("Finished reading dag runs aggregation by status",
		"duration", time.Since(start))
	return result, nil

}

// Reads latest N DAG runs with information about number of tasks completed and
// tasks overall.
func (c *Client) ReadDagRunsWithTaskInfo(ctx context.Context, latest int) ([]DagRunWithTaskInfo, error) {
	start := time.Now()
	c.logger.Debug("Start reading latest dag runs with task info", "latest", latest)
	result := make([]DagRunWithTaskInfo, 0, latest)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunsWithTaskInfoQuery(),
		statusSuccess, latest)
	if qErr != nil {
		c.logger.Error("Failed querying dag run to be scheduled", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing latest dag runs with task info",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		drti, scanErr := parseDagRunWithTaskInfo(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a dagrun with task info record", "err", scanErr)
			return nil, scanErr
		}
		result = append(result, drti)
	}
	c.logger.Debug("Finished reading dag runs with task info", "latest", latest, "duration",
		time.Since(start))
	return result, nil
}

func parseDagRun(rows Scannable) (DagRun, error) {
	var runId int64
	var dagId, execTs, insertTs, status, statusTs, version string

	scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status,
		&statusTs, &version)
	if scanErr != nil {
		return DagRun{}, scanErr
	}
	dagrun := DagRun{
		RunId:          runId,
		DagId:          dagId,
		ExecTs:         execTs,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	return dagrun, nil
}

func parseDagRunWithTaskInfo(rows Scannable) (DagRunWithTaskInfo, error) {
	var runId int64
	var taskNum, taskCompleted int
	var dagId, execTs, insertTs, status, statusTs, version string

	scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status,
		&statusTs, &version, &taskNum, &taskCompleted)
	if scanErr != nil {
		return DagRunWithTaskInfo{}, scanErr
	}
	dagrun := DagRun{
		RunId:          runId,
		DagId:          dagId,
		ExecTs:         execTs,
		InsertTs:       insertTs,
		Status:         status,
		StatusUpdateTs: statusTs,
		Version:        version,
	}
	dagrunWithTaskInfo := DagRunWithTaskInfo{
		DagRun:           dagrun,
		TaskNum:          taskNum,
		TaskCompletedNum: taskCompleted,
	}
	return dagrunWithTaskInfo, nil
}

func (c *Client) readDagRunsQuery(topN int) string {
	if topN == -1 {
		return `
			SELECT
				RunId,
				DagId,
				ExecTs,
				InsertTs,
				Status,
				StatusUpdateTs,
				Version
			FROM
				dagruns
			WHERE
				DagId = ?
			ORDER BY
				RunId DESC
		`
	}
	return `
		SELECT
			RunId,
			DagId,
			ExecTs,
			InsertTs,
			Status,
			StatusUpdateTs,
			Version
		FROM
			dagruns
		WHERE
			DagId = ?
		ORDER BY
			RunId DESC
		LIMIT
			?
	`
}

func (c *Client) insertDagRunQuery() string {
	return `
		INSERT INTO dagruns (DagId, ExecTs, InsertTs, Status, StatusUpdateTs, Version)
		VALUES (?, ?, ?, ?, ?, ?)
	`
}

func (c *Client) latestDagRunsQuery() string {
	return `
		WITH latestDagRuns AS (
			SELECT
				DagId,
				MAX(RunId) AS LatestRunId
			FROM
				dagruns
			GROUP BY
				DagId
		)
		SELECT
			d.RunId,
			d.DagId,
			d.ExecTs,
			d.InsertTs,
			d.Status,
			d.StatusUpdateTs,
			d.Version
		FROM
			dagruns d
		INNER JOIN
			latestDagRuns ldr ON d.RunId = ldr.LatestRunId
	`
}

func (c *Client) readDagRunByExecTsQuery() string {
	return `
		SELECT
			RunId,
			DagId,
			ExecTs,
			InsertTs,
			Status,
			StatusUpdateTs,
			Version
		FROM
			dagruns
		WHERE
			DagId = ? AND ExecTs = ?
	`
}

func (c *Client) updateDagRunStatusQuery() string {
	return `
	UPDATE
		dagruns
	SET
		Status = ?,
		StatusUpdateTs = ?
	WHERE
		RunId = ?
	`
}

func (c *Client) updateDagRunStatusByExecTsQuery() string {
	return `
	UPDATE
		dagruns
	SET
		Status = ?,
		StatusUpdateTs = ?
	WHERE
			DagId = ?
		AND ExecTs = ?
	`
}

func (c *Client) readDagRunNotFinishedQuery() string {
	return `
		SELECT
			RunId,
			DagId,
			ExecTs,
			InsertTs,
			Status,
			StatusUpdateTs,
			Version
		FROM
			dagruns
		WHERE
			Status NOT IN (?, ?)
		ORDER BY
			RunId ASC
	`
}

func (c *Client) readDagRunsAggByStatus() string {
	return `
		SELECT
			Status,
			COUNT(*) AS CNT
		FROM
			dagruns
		GROUP BY
			Status
	`
}

func (c *Client) readDagRunsWithTaskInfoQuery() string {
	return `
		SELECT
			dr.RunId,
			MAX(dr.DagId) AS DagId,
			MAX(dr.ExecTs) AS ExecTs,
			MAX(dr.InsertTs) AS InsertTs,
			MAX(dr.Status) AS Status,
			MAX(dr.StatusUpdateTs) AS StatusUpdateTs,
			MAX(dr.Version) AS Version,
			COUNT(dt.TaskId) AS TaskNum,
			COUNT(DISTINCT drt.TaskId) AS TaskCompletedNum
		FROM
			dagruns dr
		LEFT JOIN
			dagtasks dt ON dr.DagId = dt.DagId AND dt.IsCurrent = 1
		LEFT JOIN
			dagruntasks drt ON
					drt.DagId = dr.DagId
				AND drt.ExecTs = dr.ExecTs
				AND drt.TaskId = dt.TaskId
				AND drt.Status = ?
		WHERE
			dr.RunId > (SELECT MAX(RunId) As LatestRunId FROM dagruns) - ?
		GROUP BY
			dr.RunId
		ORDER BY
			dr.RunId DESC
	`
}

func (c *Client) readDagRunWithTaskInfoQuery() string {
	return `
		SELECT
			d.RunId,
			d.DagId,
			d.ExecTs,
			d.InsertTs,
			d.Status,
			d.StatusUpdateTs,
			d.Version
		FROM
			dagruns d
		WHERE
			d.RunId = ?
	`
}
