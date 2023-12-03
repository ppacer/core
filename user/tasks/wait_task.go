package tasks

import (
	"log/slog"
	"time"
)

// WaitTask is a Task which just waits and logs.
type WaitTask struct {
	TaskId   string
	Interval time.Duration
}

func (wt WaitTask) Id() string { return wt.TaskId }

func (wt WaitTask) Execute() {
	slog.Info("Start sleeping", "task", wt.Id(), "interval", wt.Interval)
	time.Sleep(wt.Interval)
	slog.Info("Task is done", "task", wt.Id())
}
