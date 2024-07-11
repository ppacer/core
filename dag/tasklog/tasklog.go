// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package tasklog

import (
	"context"
	"log/slog"
	"time"
)

// TaskInfo contains information about task context including DAG run
// information.
type TaskInfo struct {
	DagId  string
	ExecTs time.Time
	TaskId string
	Retry  int
}

// Record represents single dag.Task log record. It doesn't contain
// information about DAG run task, because Reader is instantiated for
// given TaskInfo.
type Record struct {
	Level      string
	InsertTs   time.Time
	Message    string
	Attributes map[string]any
}

// Reader represents a reader for single dag.Task log records.
// TODO(dskrzypiec): this interface will be revised after frontend
// implementation for fetching task logs.
type Reader interface {
	ReadAll(context.Context) ([]Record, error)
	ReadLatest(context.Context, int) ([]Record, error)
}

// Factory represents an object which can create loggers and log
// readers for given DAG run tasks. Usually there should be at least one
// implementation of this interface for supported by ppacer persistence layer
// (like SQLite, PostgreSQL, etc).
type Factory interface {
	GetLogger(TaskInfo) *slog.Logger
	GetLogReader(TaskInfo) Reader
}
