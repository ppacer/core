package sched

import (
	"context"
	"fmt"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/timeutils"
	"github.com/rs/zerolog/log"
)

type DagRunTask struct {
	DagRun
	TaskId string
}

type taskScheduler struct {
	DbClient    *db.Client
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

type taskSchedulerError struct {
	DagId  dag.Id
	ExecTs time.Time
	Err    error
}

// Start starts taskScheduler loop which gets DAG runs from the queue and start
// scheduling it in a separate goroutines.
func (ts *taskScheduler) Start() {
	taskSchedulerErrors := make(chan taskSchedulerError, 1000)
	for {
		select {
		case err := <-taskSchedulerErrors:
			// TODO(dskrzypiec): What do we want to do with those errors?
			log.Error().Str("dagId", string(err.DagId)).
				Time("execTs", err.ExecTs).Err(err.Err)
		default:
		}
		if ts.DagRunQueue.Size() == 0 {
			// DagRunQueue is empty, we wait for a bit and then we'll try again
			time.Sleep(time.Duration(ts.Config.HeartbeatMs) * time.Millisecond)
			continue
		}
		dagrun, err := ts.DagRunQueue.Pop()
		if err == ds.ErrQueueIsEmpty {
			continue
		}
		if err != nil {
			// TODO: should we do anything else? Probably not, because item
			// should be probably still on the queue
			log.Error().Err(err).Msgf("[%s] Error while getting dag run from the queue. Will try again.",
				LOG_PREFIX)

			continue
		}
		// TODO(dskrzypiec): Think about context. At least we need to add
		// timeout for overall DAG run timeout. Start scheduling new DAG run in
		// a separate goroutine.
		go ts.scheduleDagTasks(context.TODO(), dagrun, ts.TaskQueue,
			taskSchedulerErrors)
	}
}

// Function scheduleDagTasks is responsible for scheduling tasks of single DAG
// run. Each call to this function by taskScheduler is fire up in separate
// goroutine.
func (ts *taskScheduler) scheduleDagTasks(
	ctx context.Context,
	dagrun DagRun,
	tasks ds.Queue[DagRunTask],
	errorsChan chan taskSchedulerError,
) {
	dagId := string(dagrun.DagId)
	log.Debug().Str("dagId", dagId).Time("execTs", dagrun.AtTime).
		Msgf("[%s] Start scheduling tasks...", LOG_PREFIX)
	execTs := timeutils.ToString(dagrun.AtTime)

	// Update dagrun state to running
	stateUpdateErr := ts.DbClient.UpdateDagRunStatusByExecTs(
		ctx, dagId, execTs, db.DagRunStatusRunning,
	)
	if stateUpdateErr != nil {
		// TODO(dskrzypiec): should we add retries? Probably...
		errorsChan <- taskSchedulerError{
			DagId:  dagrun.DagId,
			ExecTs: dagrun.AtTime,
			Err:    stateUpdateErr,
		}
		// just send error, we don't want to stop scheduling because of dagrun
		// status failure
	}

	// Schedule tasks, starting from root - put on the task queue
	dag, dagGetErr := dag.Get(dagrun.DagId)
	if dagGetErr != nil {
		err := fmt.Errorf("cannot get DAG %s from DAG registry: %s", dagId,
			dagGetErr.Error())
		errorsChan <- taskSchedulerError{
			DagId:  dagrun.DagId,
			ExecTs: dagrun.AtTime,
			Err:    err,
		}
		return
	}
	if dag.Root == nil {
		log.Warn().Str("dagId", string(dagrun.DagId)).
			Msgf("[%s] DAG %s has no tasks. There is nothing to schedule",
				LOG_PREFIX, string(dagrun.DagId))
		return
	}

	// TODO: MAIN SCHEDULING LOGIC HERE
	// We need to efficiently store map[{DagId, ExecTs,
	// TaskID}]DagRunTaskStatus to check if we can proceed within the DAG to
	// schedule next tasks.

	// Step 3: Check (how?) whenever dependencies for the next task are met. Go
	// to Step 2.

	// Step 4: Update dagrun state to finished
}
