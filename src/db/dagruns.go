package db

import (
	"time"

	"github.com/rs/zerolog/log"
)

type DagRun struct {
	RunId    int64
	DagId    string
	ExecTs   string
	InsertTs string
	Status   string
}

const (
	DagRunStatusScheduled = "SCHEDULED"
)

// InsertDagRun inserts new row into dagruns table for given DagId and execution timestamp. Initial status is set to
// DagRunStatusScheduled. RunId for just inserted dag run is returned or -1 in case when error is not nil.
func (c *Client) InsertDagRun(dagId, execTs string) (int64, error) {
	start := time.Now()
	insertTs := time.Now().Format(InsertTsFormat)
	log.Info().Str("dagId", dagId).Str("execTs", insertTs).Msgf("[%s] Start inserting dag run...", LOG_PREFIX)
	res, err := c.dbConn.Exec(c.insertDagRunQuery(), dagId, execTs, insertTs, DagRunStatusScheduled)
	if err != nil {
		log.Error().Err(err).Str("dagId", dagId).Str("execTs", execTs).Msgf("[%s] Cannot insert new dag run", LOG_PREFIX)
		return -1, err
	}
	log.Info().Str("dagId", dagId).Str("execTs", execTs).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished inserting dag run in state SCHEDULED", LOG_PREFIX)
	return res.LastInsertId()
}

func (c *Client) insertDagRunQuery() string {
	return `
		INSERT INTO dagruns (DagId, ExecTs, InsertTs, Status)
		VALUES (?, ?, ?, ?)
	`
}
