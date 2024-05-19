package tasklog

import (
	"log/slog"
	"time"

	"github.com/ppacer/core/dag"
)

// TaskLogRecord represents single dag.Task log record. It doesn't contain
// information about DAG run task, because TaskLogReader is instantiated for
// give dag.TaskRunInfo context.
type TaskLogRecord struct {
	Level      string
	InsertTs   time.Time
	Message    string
	Attributes map[string]any
}

// TaskLogReader represents a reader for single dag.Task log records.
// TODO(dskrzypiec): this interface will be revised after frontend
// implementation for fetching task logs.
type TaskLogReader interface {
	ReadAll() ([]TaskLogRecord, error)
	ReadLatest(int) ([]TaskLogRecord, error)
}

// TaskLoggerFactory represents an object which can create loggers and log
// readers for given DAG run tasks. Usually there should be at least one
// implementation of this interface for supported by ppacer persistence layer
// (like SQLite, PostgreSQL, etc).
type TaskLoggerFactory interface {
	GetLogger(dag.TaskRunInfo) *slog.Logger
	GetLogReader(dag.TaskRunInfo) TaskLogReader
}
