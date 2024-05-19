// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package tasklog implements ppacer Task loggers.
package tasklog

import (
	"context"
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

// TODO
type SQLite struct {
	dbClient   *db.Client
	loggerOpts *slog.HandlerOptions
}

func (s *SQLite) GetLogger(tri dag.TaskRunInfo) *slog.Logger {
	ri := dag.RunInfo{DagId: tri.DagId, ExecTs: tri.ExecTs}
	return NewSQLiteLogger(ri, tri.TaskId, s.dbClient, s.loggerOpts)
}

// TODO
func (s *SQLite) GetLogReader(tri dag.TaskRunInfo) TaskLogReader {
	// TODO
	return nil
}

// NewSQLiteLogger instantiate new structured logger instance for writing DAG
// run task logs into SQLite database. It writes logs into tasklogs table which
// is indexed by date and DAG run task metadata.
func NewSQLiteLogger(ri dag.RunInfo, taskId string, dbClient *db.Client, opts *slog.HandlerOptions) *slog.Logger {
	sw := sqliteLogWriter{ri: ri, taskId: taskId, dbClient: dbClient}
	return slog.New(slog.NewJSONHandler(&sw, opts))
}

type sqliteLogReader struct {
	tri      dag.TaskRunInfo
	dbClient *db.Client
	timeout  time.Duration
}

// ReadAll reads all log records for the DAG run task context in chronological
// order.
func (s *sqliteLogReader) ReadAll() ([]TaskLogRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	logs, readErr := s.dbClient.ReadDagRunTaskLogs(
		ctx, string(s.tri.DagId), timeutils.ToString(s.tri.ExecTs), s.tri.TaskId,
	)
	if readErr != nil {
		return nil, readErr
	}
	return toTaskLogRecords(logs)
}

// ReadLatest reads n latest log records for the DAG run task context in
// chronological order.
func (s *sqliteLogReader) ReadLatest(n int) ([]TaskLogRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	logs, readErr := s.dbClient.ReadDagRunTaskLogsLatest(
		ctx, string(s.tri.DagId), timeutils.ToString(s.tri.ExecTs),
		s.tri.TaskId, n,
	)
	if readErr != nil {
		return nil, readErr
	}
	return toTaskLogRecords(logs)
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
		return 0, fmt.Errorf("cannot deserialize JSON from slog.JSONHandler: %w",
			jErr)
	}
	_, tErr := getKeyAndDelete(fields, timeFieldKey)
	if tErr != nil {
		return 0, tErr
	}
	lvl, lErr := getKeyAndDelete(fields, levelFieldKey)
	if lErr != nil {
		return 0, lErr
	}
	msg, mErr := getKeyAndDelete(fields, messageFieldKey)
	if mErr != nil {
		return 0, mErr
	}

	fieldsJson, jErr := json.Marshal(fields)
	if jErr != nil {
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

func toTaskLogRecords(tlrs []db.TaskLogRecord) ([]TaskLogRecord, error) {
	newTlrs := make([]TaskLogRecord, len(tlrs))
	for idx, rec := range tlrs {
		newRec, err := toTaskLogRecord(rec)
		if err != nil {
			return newTlrs, err
		}
		newTlrs[idx] = newRec
	}
	return newTlrs, nil
}

func toTaskLogRecord(tlr db.TaskLogRecord) (TaskLogRecord, error) {
	var attrs map[string]any
	jErr := json.Unmarshal([]byte(tlr.Attributes), &attrs)
	if jErr != nil {
		return TaskLogRecord{}, jErr
	}
	newTlr := TaskLogRecord{
		Level:      tlr.Level,
		InsertTs:   timeutils.FromStringMust(tlr.InsertTs),
		Message:    tlr.Message,
		Attributes: attrs,
	}
	return newTlr, nil
}
