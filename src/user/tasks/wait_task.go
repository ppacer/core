package tasks

import (
	"time"

	"github.com/rs/zerolog/log"
)

// WaitTask is a Task which just waits and logs.
type WaitTask struct {
	TaskId   string
	Interval time.Duration
}

func (wt WaitTask) Id() string { return wt.TaskId }

func (wt WaitTask) Execute() {
	log.Info().Msgf("Task [%s] starts sleeping for %v...", wt.Id(), wt.Interval)
	time.Sleep(wt.Interval)
	log.Info().Msgf("Task [%s] is done", wt.Id())
}
