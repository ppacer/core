// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package e2etests

import (
	"errors"
	"time"

	"github.com/ppacer/core/dag"
)

// Empty task with no action.
type emptyTask struct {
	taskId string
}

func (et emptyTask) Id() string                      { return et.taskId }
func (et emptyTask) Execute(_ dag.TaskContext) error { return nil }

// Task with just waiting action.
type waitTask struct {
	taskId   string
	interval time.Duration
}

func (wt waitTask) Id() string { return wt.taskId }

func (wt waitTask) Execute(_ dag.TaskContext) error {
	time.Sleep(wt.interval)
	return nil
}

// Task which logs.
type logTask struct {
	taskId string
}

func (lt logTask) Id() string { return lt.taskId }

func (lt logTask) Execute(tc dag.TaskContext) error {
	tc.Logger.Warn("Test log message", "taskId", lt.taskId, "dagrun", tc.DagRun)
	return nil
}

// Task which always return non-nil error.
type errTask struct {
	taskId string
}

func (et errTask) Id() string { return et.taskId }

func (et errTask) Execute(tc dag.TaskContext) error {
	return errors.New("task failed")
}

// Task which always panics with runtime exception
type runtimeErrTask struct {
	taskId string
}

func (ret runtimeErrTask) Id() string { return ret.taskId }

func (ret runtimeErrTask) Execute(tc dag.TaskContext) error {
	one := 1
	zero := 1 - one
	tc.Logger.Info("Test", "number", 42/zero)
	return nil
}
