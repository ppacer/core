package db

import (
	"go_shed/src/timeutils"
	"go_shed/src/version"
	"time"

	"github.com/rs/zerolog/log"
)

type DagRun struct {
	RunId    int64
	DagId    string
	ExecTs   string
	InsertTs string
	Status   string
	Version  string
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
		var dagId, execTs, insertTs, status, version string

		scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status)
		if scanErr != nil {
			log.Error().Err(scanErr).Str("dagId", dagId).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
		}
		dagrun := DagRun{
			RunId:    runId,
			DagId:    dagId,
			ExecTs:   execTs,
			InsertTs: insertTs,
			Status:   status,
			Version:  version,
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
	res, err := c.dbConn.Exec(c.insertDagRunQuery(), dagId, execTs, insertTs, DagRunStatusScheduled, version.Version)
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
		var dagId, execTs, insertTs, status, version string

		scanErr := rows.Scan(&runId, &dagId, &execTs, &insertTs, &status)
		if scanErr != nil {
			log.Error().Err(scanErr).Msgf("[%s] Failed scanning a DagRun record.", LOG_PREFIX)
			return nil, scanErr
		}
		dagrun := DagRun{
			RunId:    runId,
			DagId:    dagId,
			ExecTs:   execTs,
			InsertTs: insertTs,
			Status:   status,
			Version:  version,
		}
		dagruns = append(dagruns, dagrun)
	}

	log.Info().Dur("durationMs", time.Since(start)).Msgf("[%s] Finished reading latest DAG runs.", LOG_PREFIX)
	return dagruns, nil
}

func (c *Client) UpdateDagRunStatus(runId int64, status string) error {
	// TODO
	return nil
}

func (c *Client) DagRunExists(dagId, execTs string) (bool, error) {
	// TODO
	return false, nil
}

func (c *Client) readDagRunsQuery(topN int) string {
	if topN == -1 {
		return `
			SELECT
				RunId,
				DagId,
				ExecTs,
				InsertTs,
				Status
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
			Status
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
		INSERT INTO dagruns (DagId, ExecTs, InsertTs, Status, Version)
		VALUES (?, ?, ?, ?, ?)
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
			d.Status
		FROM
			dagruns d
		INNER JOIN
			latestDagRuns ldr ON d.RunId = ldr.LatestRunId
	`
}
