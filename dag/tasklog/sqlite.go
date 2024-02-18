package tasklog

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"unsafe"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

const (
	timeFieldKey    = "time"
	levelFieldKey   = "level"
	messageFieldKey = "msg"
)

// NewSQLiteLogger instantiate new structured logger instance for writing DAG
// run task logs into SQLite database. It writes logs into tasklogs table which
// is indexed by date and DAG run task metadata.
func NewSQLiteLogger(ri dag.RunInfo, taskId string, dbClient *db.Client, opts *slog.HandlerOptions) *slog.Logger {
	sw := sqliteLogWriter{ri: ri, taskId: taskId, dbClient: dbClient}
	return slog.New(slog.NewJSONHandler(&sw, opts))
}

type sqliteLogWriter struct {
	ri       dag.RunInfo
	taskId   string
	dbClient *db.Client
}

// Write parse and writes given input to SQLite database tasklogs table.
// Expected input is JSON produced by slog.JSONHandler or equivalent format.
func (s *sqliteLogWriter) Write(p []byte) (int, error) {
	var fields map[string]any
	jErr := json.Unmarshal(p, &fields)
	if jErr != nil {
		slog.Error("cannot deserialize JSON from slog.JSONHandler", "input",
			string(p), "err", jErr.Error())
		return 0, fmt.Errorf("cannot deserialize JSON from slog.JSONHandler: %w",
			jErr)
	}
	_, tErr := getKeyAndDelete(fields, timeFieldKey)
	if tErr != nil {
		slog.Error("cannot get key from slog.JSONHandler", "key", timeFieldKey)
	}
	lvl, lErr := getKeyAndDelete(fields, levelFieldKey)
	if lErr != nil {
		slog.Error("cannot get key from slog.JSONHandler", "key", levelFieldKey)
		return 0, lErr
	}
	msg, mErr := getKeyAndDelete(fields, messageFieldKey)
	if mErr != nil {
		slog.Error("cannot get key from slog.JSONHandler", "key", messageFieldKey)
		return 0, mErr
	}

	fieldsJson, jErr := json.Marshal(fields)
	if jErr != nil {
		slog.Error("cannot serialize attributes back to JSON", "attributes",
			fields, "err", jErr.Error())
		return 0, fmt.Errorf("cannot serialize attributes to JSON: %w", jErr)
	}
	tlr := db.TaskLogRecord{
		DagId:      string(s.ri.DagId),
		ExecTs:     timeutils.ToString(s.ri.ExecTs),
		TaskId:     s.taskId,
		InsertTs:   timeutils.ToString(time.Now()),
		Level:      lvl,
		Message:    msg,
		Attributes: string(fieldsJson),
	}
	iErr := s.dbClient.InsertTaskLog(tlr)
	if iErr != nil {
		return 0, iErr
	}
	return int(unsafe.Sizeof(tlr)), nil
}

func getKeyAndDelete(m map[string]any, key string) (string, error) {
	val, tsExists := m[key]
	if !tsExists {
		return "", fmt.Errorf("missing field %s in given input", key)
	}
	valStr, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("value for key %s is not a string", key)
	}
	delete(m, key)
	return valStr, nil
}
