package db

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/dskrzypiec/scheduler/src/timeutils"
	"github.com/dskrzypiec/scheduler/src/version"
)

type DagRun struct {
	RunId          int64
	DagId          string
	ExecTs         string
	InsertTs       string
	Status         string
	StatusUpdateTs string
	Version        string
}

const (
	DagRunStatusReadyToSchedule = "READY_TO_SCHEDULE"
	DagRunStatusScheduled       = "SCHEDULED"
	DagRunStatusRunning         = "RUNNING"
	DagRunStatusSuccess         = "SUCCESS"
)

// ReadDagRuns reads topN latest dag runs for given DAG ID.
func (c *Client) ReadDagRuns(ctx context.Context, dagId string, topN int) ([]DagRun, error) {
	start := time.Now()
	slog.Debug("Start reading dag runs from DB", "dagId", dagId, "topN", topN)
	cap := topN
	if topN < 0 {
		cap = 1000
	}
	dagruns := make([]DagRun, 0, cap)
	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunsQuery(topN), dagId, topN)
	if qErr != nil {
		slog.Error("Failed querying dag runs", "dagId", dagId, "topN", topN,
			"err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			slog.Warn("Context done while processing rows", "dagId", dagId,
				"topN", topN, "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			slog.Error("Failed scanning dagrun record", "dagId", dagId, "err",
				scanErr)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}
	slog.Debug("Finished reading dag runs", "dagId", dagId, "topN", topN,
		"duration", time.Since(start))
	return dagruns, nil
}

// InsertDagRun inserts new row into dagruns table for given DagId and
// execution timestamp. Initial status is set to DagRunStatusScheduled. RunId
// for just inserted dag run is returned or -1 in case when error is not nil.
func (c *Client) InsertDagRun(ctx context.Context, dagId, execTs string) (int64, error) {
	start := time.Now()
	insertTs := timeutils.ToString(time.Now())
	slog.Debug("Start inserting dag run", "dagId", dagId, "execTs", insertTs)
	res, err := c.dbConn.ExecContext(
		ctx, c.insertDagRunQuery(),
		dagId, execTs, insertTs, DagRunStatusScheduled, insertTs,
		version.Version,
	)
	if err != nil {
		slog.Error("Cannot insert new dag run", "dagId", dagId, "execTs", execTs,
			"err", err)
		return -1, err
	}
	slog.Debug("Finished inserting dag run in state SCHEDULED", "dagId", dagId,
		"execTs", execTs, "duration", time.Since(start))
	return res.LastInsertId()
}

// ReadLatestDagRuns reads latest dag run for each Dag. Returns map from DagId
// to DagRun.
func (c *Client) ReadLatestDagRuns(ctx context.Context) (map[string]DagRun, error) {
	start := time.Now()
	slog.Debug("Start reading latest run for each DAG from dagruns table")
	dagruns := make(map[string]DagRun, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.latestDagRunsQuery())
	if qErr != nil {
		slog.Error("Failed querying latest DAG runs", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			slog.Warn("Context done while processing latest dag run rows", "err",
				ctx.Err())
			return nil, ctx.Err()
		default:
		}

		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			slog.Error("Failed scanning dagrun record", "err", scanErr)
			return nil, scanErr
		}
		dagruns[dagrun.DagId] = dagrun
	}
	slog.Debug("Finished reading latest dag runs", "duration", time.Since(start))
	return dagruns, nil
}

// Updates dagrun status for given runId.
func (c *Client) UpdateDagRunStatus(
	ctx context.Context, runId int64, status string,
) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	slog.Debug("Start updating dag run status", "runId", runId)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunStatusQuery(), status, updateTs, runId,
	)
	if err != nil {
		slog.Error("Cannot update dag run", "runId", runId, "status", status,
			"err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		slog.Error("Seems that too many rows were updated. Expected exactly one",
			"runId", runId, "rowsUpdated", rowsUpdated)
		return errors.New("too many rows updated")
	}
	slog.Debug("Finished updating dag run", "runId", runId, "status", status,
		"duration", time.Since(start))
	return nil
}

// Updates dagrun status for given dagId and execTs (when runId is not
// available). Pair (dagId, execTs) is unique in dagrun table.
func (c *Client) UpdateDagRunStatusByExecTs(
	ctx context.Context, dagId, execTs, status string,
) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	slog.Debug("Start updating dag run status by execTs", "dagId", dagId,
		"execTs", execTs, "status", status)
	res, err := c.dbConn.ExecContext(
		ctx, c.updateDagRunStatusByExecTsQuery(),
		status, updateTs, dagId, execTs,
	)
	if err != nil {
		slog.Error("Cannot update dag run", "dagId", dagId, "execTs", execTs,
			"status", status, "err", err)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		slog.Error("Seems that too many rows were updated. Expected exactly one",
			"dagId", dagId, "execTs", execTs, "status", status, "rowsUpdated",
			rowsUpdated)
		return errors.New("too many rows updated")
	}
	slog.Debug("Finished updating dag run", "dagId", dagId, "execTs", execTs,
		"status", status, "duration", time.Since(start))
	return nil
}

// DagRunExists checks whenever dagrun already exists for given DAG ID and
// schedule timestamp.
func (c *Client) DagRunExists(
	ctx context.Context, dagId, execTs string,
) (bool, error) {
	start := time.Now()
	slog.Debug("Start DagRunExists query", "dagId", dagId, "execTs", execTs)

	q := "SELECT COUNT(*) FROM dagruns WHERE DagId=? AND ExecTs=? AND (Status=? OR Status=?)"
	row := c.dbConn.QueryRowContext(
		ctx, q, dagId, execTs, DagRunStatusScheduled,
		DagRunStatusReadyToSchedule,
	)
	var count int
	err := row.Scan(&count)
	if err != nil {
		slog.Error("Cannot execute DagRunExists query", "dagId", dagId,
			"execTs", execTs, "err", err)
		return false, err
	}
	slog.Debug("Finished DagRunExists query", "dagId", dagId, "execTs", execTs,
		"duration", time.Since(start))
	return count > 0, nil
}

// Reads dag run from dagruns table which are in statuse READY_TO_SCHEDULE and
// SCHEDULED.
func (c *Client) ReadDagRunsToBeScheduled(ctx context.Context) ([]DagRun, error) {
	start := time.Now()
	slog.Debug("Start reading dag runs that should be scheduled")
	dagruns := make([]DagRun, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunToBeScheduledQuery(),
		DagRunStatusReadyToSchedule, DagRunStatusScheduled)
	if qErr != nil {
		slog.Error("Failed querying dag run to be scheduled", "err", qErr)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			slog.Warn("Context done while processing dag runs to be scheduled",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			slog.Error("Failed scanning a dagrun record", "err", scanErr)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}
	slog.Debug("Finished reading dag runs to be scheduled", "duration",
		time.Since(start))
	return dagruns, nil
}

func parseDagRun(rows *sql.Rows) (DagRun, error) {
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

func (c *Client) readDagRunToBeScheduledQuery() string {
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
			Status = ? OR Status = ?
		ORDER BY
			RunId ASC
	`
}
