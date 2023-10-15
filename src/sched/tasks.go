package sched

import (
	"go_shed/src/ds"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type DagRunTask struct {
	DagRun
	TaskId string
}

type taskScheduler struct {
	DagRunQueue ds.Queue[DagRun]
	TaskQueue   ds.Queue[DagRunTask]
	Config      taskSchedulerConfig
}

type taskSchedulerConfig struct {
	MaxConcurrentDagRuns int
	HeartbeatMs          int
}

func defaultTaskSchedulerConfig() taskSchedulerConfig {
	return taskSchedulerConfig{
		MaxConcurrentDagRuns: 1000,
		HeartbeatMs:          1,
	}
}

func (ts *taskScheduler) Start() {
	var runningDagRuns *atomic.Int32
	for {
		if ts.DagRunQueue.Size() == 0 || runningDagRuns.Load() >= int32(ts.Config.MaxConcurrentDagRuns) {
			time.Sleep(time.Duration(ts.Config.HeartbeatMs) * time.Millisecond)
			continue
		}
		dagrun, err := ts.DagRunQueue.Pop()
		if err == ds.ErrQueueIsEmpty {
			continue
		}
		if err != nil {
			// TODO: should we do anything else? Probably not, because item should be probably still on the queue
			log.Error().Err(err).Msgf("[%s] Error while getting dag run from the queue", LOG_PREFIX)
			continue
		}
		go ts.scheduleDagTasks(dagrun, ts.TaskQueue, runningDagRuns)
	}
}

func (ts *taskScheduler) scheduleDagTasks(dagrun DagRun, tasks ds.Queue[DagRunTask], runningDagRuns *atomic.Int32) {
	//start := time.Now()
	log.Debug().Str("dagId", string(dagrun.DagId)).Time("execTs", dagrun.AtTime).Msgf("[%s] Start scheduling tasks...", LOG_PREFIX)

	// Step 1: Update dagrun state to running

	// Step 2: Schedule tasks, starting from root - put on the task queue

	// Step 3: Check (how?) whenever dependencies for the next task are met. Go to Step 2.

	// Step 4: Update dagrun state to finished
	runningDagRuns.Add(-1)
}
