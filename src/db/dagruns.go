package db

import (
	"database/sql"
	"errors"
	"go_shed/src/timeutils"
	"go_shed/src/version"
	"time"

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
	DagRunStatusScheduled = "SCHEDULED"
)

func (c *Client) ReadDagRuns(dagId string, topN int) ([]DagRun, error) {
	start := time.Now()
	log.Info().Str("dagId", dagId).Int("topN", topN).Msgf("[%s] Start reading DAG runs...", LOG_PREFIX)
	cap := topN
	if topN < 0 {
		cap = 1000
	}
	dagruns := make([]DagRun, 0, cap)

	rows, qErr := c.dbConn.Query(c.readDagRunsQuery(topN), dagId, topN)
	if qErr != nil {
		log.Error().Err(qErr).Str("dagId", dagId).Int("topN", topN).Msgf("[%s] Failed querying DAG runs.", LOG_PREFIX)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		var runId int64
		var dagId, execTs, insertTs, status, statusTs, version string

		scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status, &statusTs, &version)
		if scanErr != nil {
			log.Error().Err(scanErr).Str("dagId", dagId).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
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
		dagruns = append(dagruns, dagrun)
	}

	log.Info().Str("dagId", dagId).Int("topN", topN).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished reading DAG runs.", LOG_PREFIX)
	return dagruns, nil
}

// InsertDagRun inserts new row into dagruns table for given DagId and execution timestamp. Initial status is set to
// DagRunStatusScheduled. RunId for just inserted dag run is returned or -1 in case when error is not nil.
func (c *Client) InsertDagRun(dagId, execTs string) (int64, error) {
	start := time.Now()
	insertTs := timeutils.ToString(time.Now())
	log.Info().Str("dagId", dagId).Str("execTs", insertTs).Msgf("[%s] Start inserting dag run...", LOG_PREFIX)
	res, err := c.dbConn.Exec(c.insertDagRunQuery(), dagId, execTs, insertTs, DagRunStatusScheduled, insertTs, version.Version)
	if err != nil {
		log.Error().Err(err).Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Cannot insert new dag run", LOG_PREFIX)
		return -1, err
	}
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished inserting dag run in state SCHEDULED", LOG_PREFIX)
	return res.LastInsertId()
}

// ReadLatestDagRuns reads latest dag run for each Dag.
func (c *Client) ReadLatestDagRuns() ([]DagRun, error) {
	start := time.Now()
	log.Info().Msgf("[%s] Start reading latest run for each DAG...", LOG_PREFIX)
	dagruns := make([]DagRun, 0, 100)

	rows, qErr := c.dbConn.Query(c.latestDagRunsQuery())
	if qErr != nil {
		log.Error().Err(qErr).Msgf("[%s] Failed querying latest DAG runs.", LOG_PREFIX)
		return nil, qErr
	}
	defer rows.Close()

	for rows.Next() {
		var runId int64
		var dagId, execTs, insertTs, status, statusTs, version string

		scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status, &statusTs, &version)
		if scanErr != nil {
			log.Error().Err(scanErr).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
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
		dagruns = append(dagruns, dagrun)
	}

	log.Info().Dur("durationMs", time.Since(start)).Msgf("[%s] Finished reading latest DAG runs.", LOG_PREFIX)
	return dagruns, nil
}

// Updates dagrun for given runId and status.
func (c *Client) UpdateDagRunStatus(runId int64, status string) error {
	start := time.Now()
	updateTs := timeutils.ToString(time.Now())
	log.Info().Int64("runId", runId).Str("status", status).Msgf("[%s] Start updating dag run status...", LOG_PREFIX)
	res, err := c.dbConn.Exec(c.updateDagRunStatusQuery(), status, updateTs, runId)
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
			Msgf("[%s] Seems to many rows be updated. Expected to be only one.", LOG_PREFIX)
		return errors.New("too many rows updated")
	}
	log.Info().Int64("runId", runId).Str("status", status).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished updating dag run.", LOG_PREFIX)
	return nil
}

// DagRunExists checks whenever dagrun already exists for given DAG ID and schedule timestamp.
func (c *Client) DagRunExists(dagId, execTs string) (bool, error) {
	start := time.Now()
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Start DagRunExists query.", LOG_PREFIX)

	q := "SELECT COUNT(*) FROM dagruns WHERE DagId=? AND ExecTs=?"
	row := c.dbConn.QueryRow(q, dagId, execTs)
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
