// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package tasklog

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

func TestNewSQLite(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	s := NewSQLite(c, nil)
	if s.dbClient != c {
		t.Error("Expected dbClient to be set")
	}
}

func TestSQLiteLoggerAndReaderSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	sqlite := NewSQLite(c, nil)
	const dagId = "mock_dag"
	const taskId = "task_1"
	ts := time.Now()
	ti := TaskInfo{DagId: dagId, ExecTs: ts, TaskId: taskId}
	sqliteLogger := sqlite.GetLogger(ti)
	logReader := sqlite.GetLogReader(ti)

	msgs := []string{"test", "another msg", ""}

	// Insert logs
	for _, msg := range msgs {
		sqliteLogger.Warn(msg)
	}

	// Read logs
	ctx := context.Background()
	logs, rErr := logReader.ReadAll(ctx)
	if rErr != nil {
		t.Errorf("Could not read log records: %s", rErr.Error())
	}
	if len(logs) != len(msgs) {
		t.Errorf("Expected %d log records, got: %d", len(msgs), len(logs))
	}

	for idx, msg := range msgs {
		if logs[idx].Message != msg {
			t.Errorf("Expected message [%s] for row %d, got [%s]",
				msg, idx, logs[idx])
		}
	}
}

func TestSQLiteLoggerSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const dagId = "mock_dag"
	const taskId = "task_1"
	const retry = 0
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ti := TaskInfo{DagId: dagId, ExecTs: ts, TaskId: taskId}
	sqliteLogger := NewSQLite(c, nil).GetLogger(ti)

	messages := []string{
		"test message 1",
		"",
		"test message 2",
	}
	for _, msg := range messages {
		sqliteLogger.Error(msg)
	}

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId, retry)
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
	const retry = 0
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ti := TaskInfo{DagId: dagId, ExecTs: ts, TaskId: taskId}
	sqliteLogger := NewSQLite(c, nil).GetLogger(ti)

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
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId, retry)
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
	const retry = 0
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ti := TaskInfo{DagId: dagId, ExecTs: ts, TaskId: taskId}
	opts := slog.HandlerOptions{Level: lvl}
	sqliteLogger := NewSQLite(c, &opts).GetLogger(ti)

	sqliteLogger.Debug("debug")
	sqliteLogger.Info("info")
	sqliteLogger.Warn("warn")
	sqliteLogger.Error("error")

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId, retry)
	if readErr != nil {
		t.Errorf("Error when reading tasklogs from database: %s", readErr.Error())
	}
	if len(tlrs) != expectedRows {
		t.Errorf("Expected %d log in tasklogs, got: %d", expectedRows, len(tlrs))
	}
}

func TestToTaskLogRecord(t *testing.T) {
	now := timeutils.ToString(time.Now())
	msg := "Message"
	data := []struct {
		attrString    string
		expAttributes map[string]any
	}{
		{"{}", map[string]any{}},
		{`{"x": 42.12}`, map[string]any{"x": 42.12}},
		{`{"y": "test"}`, map[string]any{"y": "test"}},
		{`{"a": null, "b": "test"}`, map[string]any{"a": nil, "b": "test"}},
	}

	for _, d := range data {
		dbtlr := db.TaskLogRecord{
			Level:      "INFO",
			InsertTs:   now,
			Message:    msg,
			Attributes: d.attrString,
		}
		tlr, err := toTaskLogRecord(dbtlr)
		if err != nil {
			t.Errorf("Error while mapping from db.TaskLogRecord to TaskLogRecord: %s",
				err.Error())
		}
		if len(tlr.Attributes) != len(d.expAttributes) {
			t.Errorf("Attributes map has different sizes: expected %d, got %d",
				len(d.expAttributes), len(tlr.Attributes))
		}
		for k, v := range d.expAttributes {
			v2, ok := tlr.Attributes[k]
			if !ok {
				t.Errorf("Key %s is missing in TaskLogRecord Attrs", k)
			}
			if v2 != v {
				t.Errorf("Attributes for key %s differs. Expected %v, got %v",
					k, v, v2)
			}
		}
	}
}
