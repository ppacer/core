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
	"unsafe"

	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

const (
	timeFieldKey    = "time"
	levelFieldKey   = "level"
	messageFieldKey = "msg"
)

// SQLite implements Factory using SQLite database as a target for task logs.
type SQLite struct {
	dbClient   *db.Client
	loggerOpts *slog.HandlerOptions
}

// NewSQLite instantiate new SQLite object. Given database client should be
// setup to use SQLite. Optionally provided loggerOpts would be used in the
// logger for task logs.
func NewSQLite(dbClient *db.Client, loggerOpts *slog.HandlerOptions) *SQLite {
	return &SQLite{
		dbClient:   dbClient,
		loggerOpts: loggerOpts,
	}
}

// GetLogger returns new instance of slog.Logger dedicated to log events
// related to given DAG run task. Logs are stored in SQLite database configured
// in NewSQLite.
func (s *SQLite) GetLogger(ti TaskInfo) *slog.Logger {
	sw := sqliteLogWriter{ti: ti, dbClient: s.dbClient}
	return slog.New(slog.NewJSONHandler(&sw, s.loggerOpts))
}

// GetLogReader returns an instance of Reader for reading log records related
// to given DAG run task.
func (s *SQLite) GetLogReader(ti TaskInfo) Reader {
	return &sqliteLogReader{
		ti:       ti,
		dbClient: s.dbClient,
	}
}

type sqliteLogReader struct {
	ti       TaskInfo
	dbClient *db.Client
}

// ReadAll reads all log records for the DAG run task context in chronological
// order.
func (s *sqliteLogReader) ReadAll(ctx context.Context) ([]Record, error) {
	logs, readErr := s.dbClient.ReadDagRunTaskLogs(
		ctx, s.ti.DagId, timeutils.ToString(s.ti.ExecTs), s.ti.TaskId,
		s.ti.Retry,
	)
	if readErr != nil {
		return nil, readErr
	}
	return toTaskLogRecords(logs)
}

// ReadLatest reads n latest log records for the DAG run task context in
// chronological order.
func (s *sqliteLogReader) ReadLatest(ctx context.Context, n int) ([]Record, error) {
	logs, readErr := s.dbClient.ReadDagRunTaskLogsLatest(
		ctx, s.ti.DagId, timeutils.ToString(s.ti.ExecTs), s.ti.TaskId,
		s.ti.Retry, n,
	)
	if readErr != nil {
		return nil, readErr
	}
	return toTaskLogRecords(logs)
}

type sqliteLogWriter struct {
	ti       TaskInfo
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
		DagId:      s.ti.DagId,
		ExecTs:     timeutils.ToString(s.ti.ExecTs),
		TaskId:     s.ti.TaskId,
		Retry:      s.ti.Retry,
		InsertTs:   timeutils.ToString(timeutils.Now()),
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

func toTaskLogRecords(tlrs []db.TaskLogRecord) ([]Record, error) {
	newTlrs := make([]Record, len(tlrs))
	for idx, rec := range tlrs {
		newRec, err := toTaskLogRecord(rec)
		if err != nil {
			return newTlrs, err
		}
		newTlrs[idx] = newRec
	}
	return newTlrs, nil
}

func toTaskLogRecord(tlr db.TaskLogRecord) (Record, error) {
	var attrs map[string]any
	jErr := json.Unmarshal([]byte(tlr.Attributes), &attrs)
	if jErr != nil {
		return Record{}, jErr
	}
	newTlr := Record{
		Level:      tlr.Level,
		InsertTs:   timeutils.FromStringMust(tlr.InsertTs),
		Message:    tlr.Message,
		Attributes: attrs,
	}
	return newTlr, nil
}
