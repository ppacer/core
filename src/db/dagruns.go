package db

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/dskrzypiec/scheduler/src/timeutils"
	"github.com/dskrzypiec/scheduler/src/version"
	"github.com/rs/zerolog/log"
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
)

// ReadDagRuns reads topN latest dag runs for given DAG ID.
func (c *Client) ReadDagRuns(ctx context.Context, dagId string, topN int) ([]DagRun, error) {
	start := time.Now()
	log.Info().Str("dagId", dagId).Int("topN", topN).Msgf("[%s] Start reading DAG runs...", LOG_PREFIX)
	cap := topN
	if topN < 0 {
		cap = 1000
	}
	dagruns := make([]DagRun, 0, cap)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunsQuery(topN), dagId, topN)
	if qErr != nil {
		log.Error().Err(qErr).Str("dagId", dagId).Int("topN", topN).Msgf("[%s] Failed querying DAG runs.", LOG_PREFIX)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			log.Warn().Err(ctx.Err()).Str("dagId", dagId).Int("topN", topN).
				Msgf("[%s] Context done while processing rows.", LOG_PREFIX)
			return nil, ctx.Err()
		default:
		}

		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			log.Error().Err(scanErr).Str("dagId", dagId).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}

	log.Info().Str("dagId", dagId).Int("topN", topN).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished reading DAG runs.", LOG_PREFIX)
	return dagruns, nil
}

// InsertDagRun inserts new row into dagruns table for given DagId and execution timestamp. Initial status is set to
// DagRunStatusScheduled. RunId for just inserted dag run is returned or -1 in case when error is not nil.
func (c *Client) InsertDagRun(ctx context.Context, dagId, execTs string) (int64, error) {
	start := time.Now()
	insertTs := timeutils.ToString(time.Now())
	log.Info().Str("dagId", dagId).Str("execTs", insertTs).Msgf("[%s] Start inserting dag run...", LOG_PREFIX)
	res, err := c.dbConn.ExecContext(ctx, c.insertDagRunQuery(), dagId, execTs, insertTs, DagRunStatusScheduled, insertTs,
		version.Version)
	if err != nil {
		log.Error().Err(err).Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Cannot insert new dag run", LOG_PREFIX)
		return -1, err
	}
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished inserting dag run in state SCHEDULED", LOG_PREFIX)
	return res.LastInsertId()
}

// ReadLatestDagRuns reads latest dag run for each Dag. Returns map from DagId to DagRun.
func (c *Client) ReadLatestDagRuns(ctx context.Context) (map[string]DagRun, error) {
	start := time.Now()
	log.Info().Msgf("[%s] Start reading latest run for each DAG...", LOG_PREFIX)
	dagruns := make(map[string]DagRun, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.latestDagRunsQuery())
	if qErr != nil {
		log.Error().Err(qErr).Msgf("[%s] Failed querying latest DAG runs.", LOG_PREFIX)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			log.Warn().Err(ctx.Err()).Msgf("[%s] Context done while processing rows.", LOG_PREFIX)
			return nil, ctx.Err()
		default:
		}

		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			log.Error().Err(scanErr).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
		}
		dagruns[dagrun.DagId] = dagrun
	}

	log.Info().Dur("durationMs", time.Since(start)).Msgf("[%s] Finished reading latest DAG runs.", LOG_PREFIX)
	return dagruns, nil
}

// Updates dagrun status for given runId.
func (c *Client) UpdateDagRunStatus(ctx context.Context, runId int64, status string) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	log.Info().Int64("runId", runId).Str("status", status).Msgf("[%s] Start updating dag run status...", LOG_PREFIX)
	res, err := c.dbConn.ExecContext(ctx, c.updateDagRunStatusQuery(), status, updateTs, runId)
	if err != nil {
		log.Error().Err(err).Int64("runId", runId).Str("status", status).Msgf("[%s] Cannot update dag run", LOG_PREFIX)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		log.Error().Int64("runId", runId).Int64("rowsUpdated", rowsUpdated).
			Msgf("[%s] Seems that too many rows were updated. Expected exactly only one.", LOG_PREFIX)
		return errors.New("too many rows updated")
	}
	log.Info().Int64("runId", runId).Str("status", status).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished updating dag run.", LOG_PREFIX)
	return nil
}

// Updates dagrun status for given dagId and execTs (when runId is not available). Pair (dagId, execTs) is unique in
// dagrun table.
func (c *Client) UpdateDagRunStatusByExecTs(ctx context.Context, dagId, execTs, status string) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Str("status", status).
		Msgf("[%s] Start updating dag run status...", LOG_PREFIX)
	res, err := c.dbConn.ExecContext(ctx, c.updateDagRunStatusByExecTsQuery(), status, updateTs, dagId, execTs)
	if err != nil {
		log.Error().Err(err).Str("dagId", dagId).Str("execTs", execTs).Str("status", status).
			Msgf("[%s] Cannot update dag run", LOG_PREFIX)
		return err
	}
	rowsUpdated, _ := res.RowsAffected()
	if rowsUpdated == 0 {
		return sql.ErrNoRows
	}
	if rowsUpdated > 1 {
		log.Error().Str("dagId", dagId).Str("execTs", execTs).Str("status", status).Int64("rowsUpdated", rowsUpdated).
			Msgf("[%s] Seems that too many rows were updated. Expected exactly only one.", LOG_PREFIX)
		return errors.New("too many rows updated")
	}
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Str("status", status).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished updating dag run.", LOG_PREFIX)
	return nil
}

// DagRunExists checks whenever dagrun already exists for given DAG ID and schedule timestamp.
func (c *Client) DagRunExists(ctx context.Context, dagId, execTs string) (bool, error) {
	start := time.Now()
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Start DagRunExists query.", LOG_PREFIX)

	q := "SELECT COUNT(*) FROM dagruns WHERE DagId=? AND ExecTs=? AND (Status=? OR Status=?)"
	row := c.dbConn.QueryRowContext(ctx, q, dagId, execTs, DagRunStatusScheduled, DagRunStatusReadyToSchedule)
	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Error().Err(err).Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Cannot execute DagRunExists query", LOG_PREFIX)
		return false, err
	}

	log.Info().Str("dagId", dagId).Str("execTs", execTs).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished DagRunExists query", LOG_PREFIX)
	return count > 0, nil
}

// Reads dag run from dagruns table which are in statuse READY_TO_SCHEDULE and SCHEDULED.
func (c *Client) ReadDagRunsToBeScheduled(ctx context.Context) ([]DagRun, error) {
	start := time.Now()
	log.Info().Msgf("[%s] Start reading dag runs that should be scheduled...", LOG_PREFIX)
	dagruns := make([]DagRun, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagRunToBeScheduledQuery(), DagRunStatusReadyToSchedule,
		DagRunStatusScheduled)
	if qErr != nil {
		log.Error().Err(qErr).Msgf("[%s] Failed querying dag runs to be scheduled.", LOG_PREFIX)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			log.Warn().Err(ctx.Err()).Msgf("[%s] Context done while processing rows.", LOG_PREFIX)
			return nil, ctx.Err()
		default:
		}

		dagrun, scanErr := parseDagRun(rows)
		if scanErr != nil {
			log.Error().Err(scanErr).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
		}
		dagruns = append(dagruns, dagrun)
	}
	log.Info().Dur("durationMs", time.Since(start)).Msgf("[%s] Finished reading dag runs to be scheduled.", LOG_PREFIX)
	return dagruns, nil
}

func parseDagRun(rows *sql.Rows) (DagRun, error) {
	var runId int64
	var dagId, execTs, insertTs, status, statusTs, version string

	scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status, &statusTs, &version)
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
