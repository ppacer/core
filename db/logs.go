package db

import (
	"fmt"
	"log/slog"
)

// LogRecord represents single row in tasklogs table.
type TaskLogRecord struct {
	Date       string
	DagId      string
	ExecTs     string
	TaskId     string
	InsertTs   string
	LogTs      string
	Level      string
	Message    string
	Attributes string
}

// InsertTaskLog inserts single log record into tasklogs table.
func (c *Client) InsertTaskLog(tlr TaskLogRecord) error {
	res, iErr := c.dbConn.Exec(c.insertTaskLogQuery(),
		tlr.Date, tlr.DagId, tlr.ExecTs, tlr.TaskId, tlr.InsertTs, tlr.LogTs,
		tlr.Level, tlr.Message, tlr.Attributes,
	)
	if iErr != nil {
		slog.Error("Cannot insert new tasklog row", "TaskLogRecord", tlr,
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

func (c *Client) insertTaskLogQuery() string {
	return `
		INSERT INTO tasklogs (
			Date, DagId, ExecTs, TaskId, InsertTs, LogTs, Level, Message, Attributes
		)
		VALUES (?,?,?,?,?,?,?,?,?)
	`
}
