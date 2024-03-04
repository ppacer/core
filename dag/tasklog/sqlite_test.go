package tasklog

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

func TestSQLiteLoggerSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const dagId = "mock_dag"
	const taskId = "task_1"
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ri := dag.RunInfo{DagId: dag.Id(dagId), ExecTs: ts}
	sqliteLogger := NewSQLiteLogger(ri, taskId, c, nil)

	messages := []string{
		"test message 1",
		"",
		"test message 2",
	}
	for _, msg := range messages {
		sqliteLogger.Error(msg)
	}

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if readErr != nil {
		t.Errorf("Error when reading tasklogs from database: %s", readErr.Error())
	}
	if len(tlrs) != len(messages) {
		t.Errorf("Expected %d logs in tasklogs, got: %d", len(messages),
			len(tlrs))
	}
	for idx, msg := range messages {
		if tlrs[idx].Message != msg {
			t.Errorf("For log %d expected message [%s], got [%s]",
				idx, msg, tlrs[idx].Message)
		}
	}
}

func TestSQLiteLoggerAttributes(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const dagId = "mock_dag"
	const taskId = "task_1"
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ri := dag.RunInfo{DagId: dag.Id(dagId), ExecTs: ts}
	sqliteLogger := NewSQLiteLogger(ri, taskId, c, nil)

	type tmp struct {
		X int     `json:"my_x"`
		Y float64 `json:"y"`
	}

	data := []struct {
		input          []any
		expectedString string
	}{
		{[]any{}, `{}`},
		{[]any{"arg_name", "value"}, `{"arg_name":"value"}`},
		{[]any{"x", 42, "y", "test"}, `{"x":42,"y":"test"}`},
		{[]any{"x", nil}, `{"x":null}`},
		{[]any{"...", []int{42, 111, -2}}, `{"...":[42,111,-2]}`},
		{[]any{"", []any{42, "x"}}, `{"":[42,"x"]}`},
		{[]any{"m", map[string]int{"x": 42}}, `{"m":{"x":42}}`},
		{[]any{"data", tmp{X: -42, Y: 3.14}}, `{"data":{"my_x":-42,"y":3.14}}`},
	}

	for _, d := range data {
		sqliteLogger.Warn("message", d.input...)
	}

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if readErr != nil {
		t.Errorf("Error when reading tasklogs from database: %s", readErr.Error())
	}
	if len(tlrs) != len(data) {
		t.Errorf("Expected %d logs in tasklogs, got: %d", len(data), len(tlrs))
	}
	for idx, d := range data {
		if tlrs[idx].Attributes != d.expectedString {
			t.Errorf("Expected attributes for %d row to be [%s], got: [%s]",
				idx, d.expectedString, tlrs[idx].Attributes)
		}
	}
}

func TestSQLiteLoggerLevelDebug(t *testing.T) {
	testSQLiteLoggerLevel(t, slog.LevelDebug, 4)
}

func TestSQLiteLoggerLevelInfo(t *testing.T) {
	testSQLiteLoggerLevel(t, slog.LevelInfo, 3)
}

func TestSQLiteLoggerLevelWarn(t *testing.T) {
	testSQLiteLoggerLevel(t, slog.LevelWarn, 2)
}

func TestSQLiteLoggerLevelError(t *testing.T) {
	testSQLiteLoggerLevel(t, slog.LevelError, 1)
}

func testSQLiteLoggerLevel(t *testing.T, lvl slog.Level, expectedRows int) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const dagId = "mock_dag"
	const taskId = "task_1"
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ri := dag.RunInfo{DagId: dag.Id(dagId), ExecTs: ts}
	opts := slog.HandlerOptions{Level: lvl}
	sqliteLogger := NewSQLiteLogger(ri, taskId, c, &opts)

	sqliteLogger.Debug("debug")
	sqliteLogger.Info("info")
	sqliteLogger.Warn("warn")
	sqliteLogger.Error("error")

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if readErr != nil {
		t.Errorf("Error when reading tasklogs from database: %s", readErr.Error())
	}
	if len(tlrs) != expectedRows {
		t.Errorf("Expected %d log in tasklogs, got: %d", expectedRows, len(tlrs))
	}
}
