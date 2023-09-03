package db

import (
	"database/sql"
	"go_shed/src/dag"
	"go_shed/src/version"
	"reflect"
	"time"

	"github.com/rs/zerolog/log"
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

// InsertDagTask inserts new DagTask which represents a task within a DAG. Using multiple InsertDagTask for dagId and
// task is a common case. Newely inserted version will have IsCurrent=1 and others will not. On database side (DagId,
// TaskId, IsCurrent) defines primary key on dagtasks table.
func (c *Client) InsertDagTask(dagId string, task dag.Task) error {
	start := time.Now()
	insertTs := time.Now().Format(InsertTsFormat)
	log.Info().Str("dagId", dagId).Str("taskId", task.Id()).Str("insertTs", insertTs).Msgf("[%s] Start inserting new DagTask.", LOG_PREFIX)
	tx, _ := c.dbConn.Begin()

	// Make IsCurrent=0 for outdated rows
	uErr := c.outdateDagTasks(tx, dagId, task.Id(), insertTs)
	if uErr != nil {
		log.Error().Err(uErr).Str("dagId", dagId).Str("taskId", task.Id()).Msgf("[%s] Cannot outdate old dagtasks", LOG_PREFIX)
		return uErr
	}

	// Insert dagtask row
	iErr := c.insertDagTask(tx, dagId, task, insertTs)
	if iErr != nil {
		log.Error().Err(iErr).Str("dagId", dagId).Str("taskId", task.Id()).Msgf("[%s] Cannot insert new dagtask", LOG_PREFIX)
		return iErr
	}

	cErr := tx.Commit()
	if cErr != nil {
		log.Error().Err(cErr).Str("dagId", dagId).Str("taskId", task.Id()).Msgf("[%s] Could not commit SQL transaction", LOG_PREFIX)
		return cErr
	}

	log.Info().Str("dagId", dagId).Str("taskId", task.Id()).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished inserting new DagTask.", LOG_PREFIX)
	return nil
}

// Insert new row in dagtasks table.
func (c *Client) insertDagTask(tx *sql.Tx, dagId string, task dag.Task, insertTs string) error {
	tTypeName := reflect.TypeOf(task).Name()
	taskBody := dag.TaskExecuteSource(task)
	taskHash := dag.TaskHash(task)

	_, err := tx.Exec(
		c.dagTaskInsertQuery(),
		dagId, task.Id(), 1, insertTs, version.Version, tTypeName, taskHash, taskBody,
	)
	if err != nil {
		return err
	}
	return nil
}

// Outdates all row in dagtasks table for given dagId and taskId which are older then given currentInsertTs timestamp.
func (c *Client) outdateDagTasks(tx *sql.Tx, dagId string, taskId string, currentInsertTs string) error {
	_, err := tx.Exec(c.dagTaskOutdateQuery(), dagId, taskId, currentInsertTs)
	if err != nil {
		return err
	}
	return nil
}

// ReadDagTask reads single row (current version) from dagtasks table for given DAG ID and task ID.
func (c *Client) ReadDagTask(dagId, taskId string) (DagTask, error) {
	start := time.Now()
	log.Info().Str("dagId", dagId).Str("taskId", taskId).Msgf("[%s] Start reading DagTask.", LOG_PREFIX)
	row := c.dbConn.QueryRow(c.readDagTaskQuery(), dagId, taskId)

	var dId, tId, typeName, insertTs, version, bodyHash, bodySource string
	var isCurrent int
	scanErr := row.Scan(&dId, &tId, &isCurrent, &insertTs, &version, &typeName, &bodyHash, &bodySource)

	if scanErr != nil {
		log.Error().Err(scanErr).Str("dagId", dagId).Str("taskId", taskId).
			Msgf("[%s] failed scanning dagtask record", LOG_PREFIX)
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

	log.Info().Str("dagId", dagId).Str("taskId", taskId).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished reading DagTask.", LOG_PREFIX)
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

func (c *Client) dagTaskInsertQuery() string {
	return `
		INSERT INTO dagtasks (
			DagId, TaskId, IsCurrent, InsertTs, Version, TaskTypeName, TaskBodyHash, TaskBodySource
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
			AND TaskId = ?
			AND InsertTs != ?
	`
}
