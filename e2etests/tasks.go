package e2etests

import (
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
