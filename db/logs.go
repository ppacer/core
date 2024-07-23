package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"
)

// LogRecord represents single row in tasklogs table.
type TaskLogRecord struct {
	DagId      string
	ExecTs     string
	TaskId     string
	Retry      int
	InsertTs   string
	Level      string
	Message    string
	Attributes string
}

// InsertTaskLog inserts single log record into tasklogs table.
func (c *LogsClient) InsertTaskLog(tlr TaskLogRecord) error {
	res, iErr := c.dbConn.Exec(c.insertTaskLogQuery(),
		tlr.DagId, tlr.ExecTs, tlr.TaskId, tlr.Retry, tlr.InsertTs, tlr.Level,
		tlr.Message, tlr.Attributes,
	)
	if iErr != nil {
		c.logger.Error("Cannot insert new tasklog row", "TaskLogRecord", tlr,
			"err", iErr)
	}
	rowsAffected, raErr := res.RowsAffected()
	if raErr != nil {
		return fmt.Errorf("it seems like database %s does not support RowsAffected: %w",
			c.dbConn.DataSource(), raErr)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("expected single row to be inserted, got rows affected: %d",
			rowsAffected)
	}
	return nil
}

// ReadDagRunLogs reads all task logs for given DAG run in chronological order.
func (c *LogsClient) ReadDagRunLogs(ctx context.Context, dagId, execTs string) ([]TaskLogRecord, error) {
	rows, rErr := c.dbConn.QueryContext(ctx, c.readDagRunLogsQuery(), dagId,
		execTs)
	if rErr != nil {
		c.logger.Error("Error while querying DAG run logs", "dagId", dagId,
			"execTs", execTs, "err", rErr.Error())
		return nil, rErr
	}
	defer rows.Close()
	logs, err := c.readTaskLogs(ctx, rows)
	if rowsErr := rows.Err(); rowsErr != nil {
		c.logger.Error("Error while processing SQL rows from tasklogs", "dagId",
			dagId, "execTs", execTs, "err", rowsErr)
		return nil, rowsErr
	}
	return logs, err
}

// ReadDagRunTaskLogs reads all logs for given DAG run task in chronological
// order.
func (c *LogsClient) ReadDagRunTaskLogs(ctx context.Context, dagId, execTs, taskId string, retry int) ([]TaskLogRecord, error) {
	rows, rErr := c.dbConn.QueryContext(ctx, c.readDagRunTaskLogsQuery(),
		dagId, execTs, taskId, retry)
	if rErr != nil {
		c.logger.Error("Error while querying DAG run task logs", "dagId", dagId,
			"execTs", execTs, "taskId", taskId, "retry", retry, "err", rErr.Error())
		return nil, rErr
	}
	defer rows.Close()
	logs, err := c.readTaskLogs(ctx, rows)
	if rowsErr := rows.Err(); rowsErr != nil {
		c.logger.Error("Error while processing SQL rows from tasklogs", "dagId",
			dagId, "execTs", execTs, "taskId", taskId, "retry", retry,
			"err", rowsErr)
		return nil, rowsErr
	}
	return logs, err
}

// ReadDagRunTaskLogsLatest reads given number of latest DAG run task logs in
// chronological order.
func (c *LogsClient) ReadDagRunTaskLogsLatest(ctx context.Context, dagId, execTs, taskId string, retry, latest int) ([]TaskLogRecord, error) {
	records, rErr := c.readTaskLogsQuery(
		ctx, c.readDagRunTaskLogsLatestQuery(), dagId, execTs, taskId, retry,
		latest,
	)
	if rErr != nil {
		return nil, rErr
	}
	slices.Reverse(records)
	return records, nil
}

func (c *LogsClient) readTaskLogsQuery(ctx context.Context, query string, args ...any) ([]TaskLogRecord, error) {
	rows, rErr := c.dbConn.QueryContext(ctx, query, args...)
	if rErr != nil {
		c.logger.Error("Error while querying DAG run task logs", "query", query,
			"err", rErr)
		return nil, rErr
	}
	defer rows.Close()
	logs, err := c.readTaskLogs(ctx, rows)
	if rowsErr := rows.Err(); rowsErr != nil {
		c.logger.Error("Error while processing SQL rows from tasklogs",
			"query", query, "err", rowsErr)
		return nil, rowsErr
	}
	return logs, err
}

// For given *sql.Rows reads and parse TaskLogRecord from presumably tasklogs
// table. Given rows should be close by the parent function.
func (c *LogsClient) readTaskLogs(ctx context.Context, rows *sql.Rows) ([]TaskLogRecord, error) {
	start := time.Now()
	c.logger.Debug("Start reading tasklogs records")
	tlrs := make([]TaskLogRecord, 0, 10)
	var retry int
	var dagId, execTs, taskId, insertTs, lvl, msg, attr string

	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing DAG run task log records",
				"err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		scanErr := rows.Scan(&dagId, &execTs, &taskId, &retry, &insertTs,
			&lvl, &msg, &attr)
		if scanErr != nil {
			c.logger.Error("Cannot parse tasklogs record", "err", scanErr.Error())
			continue
		}
		tlr := TaskLogRecord{
			DagId:      dagId,
			ExecTs:     execTs,
			TaskId:     taskId,
			Retry:      retry,
			InsertTs:   insertTs,
			Level:      lvl,
			Message:    msg,
			Attributes: attr,
		}
		tlrs = append(tlrs, tlr)
	}

	c.logger.Debug("Finished reading tasklogs records", "duration",
		time.Since(start))
	return tlrs, nil
}

func (c *LogsClient) insertTaskLogQuery() string {
	return `
		INSERT INTO tasklogs (
			DagId, ExecTs, TaskId, Retry, InsertTs, Level, Message, Attributes
		)
		VALUES (?,?,?,?,?,?,?,?)
	`
}

func (c *LogsClient) readDagRunLogsQuery() string {
	return `
		SELECT
			DagId, ExecTs, TaskId, Retry, InsertTs, Level, Message, Attributes
		FROM
			tasklogs
		WHERE
				DagId = ?
			AND ExecTs = ?
		ORDER BY
			InsertTs ASC
	`
}

func (c *LogsClient) readDagRunTaskLogsLatestQuery() string {
	return `
		SELECT
			DagId, ExecTs, TaskId, Retry, InsertTs, Level, Message, Attributes
		FROM
			tasklogs
		WHERE
				DagId = ?
			AND ExecTs = ?
			AND TaskId = ?
			AND Retry = ?
		ORDER BY
			InsertTs DESC
		LIMIT
			?
	`
}

func (c *LogsClient) readDagRunTaskLogsQuery() string {
	return `
		SELECT
			DagId, ExecTs, TaskId, Retry, InsertTs, Level, Message, Attributes
		FROM
			tasklogs
		WHERE
				DagId = ?
			AND ExecTs = ?
			AND TaskId = ?
			AND Retry = ?
		ORDER BY
			InsertTs ASC
	`
}
