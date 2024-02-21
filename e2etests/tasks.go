package e2etests

import (
	"time"

	"github.com/ppacer/core/dag"
)

// Empty task with no action.
type emptyTask struct {
	taskId string
}

func (et emptyTask) Id() string                { return et.taskId }
func (et emptyTask) Execute(_ dag.TaskContext) {}

// Task with just waiting action.
type waitTask struct {
	taskId   string
	interval time.Duration
}

func (wt waitTask) Id() string                { return wt.taskId }
func (wt waitTask) Execute(_ dag.TaskContext) { time.Sleep(wt.interval) }
