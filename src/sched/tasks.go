package sched

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/timeutils"
)

type DagRunTask struct {
	DagId  dag.Id
	AtTime time.Time
	TaskId string
}

type DagRunTaskState struct {
	Status         DagRunTaskStatus
	StatusUpdateTs time.Time
	// TODO: probably more metadata
}

type taskScheduler struct {
	DbClient    *db.Client
	DagRunQueue ds.Queue[DagRun]
	TaskQueue   ds.Queue[DagRunTask]
	TaskCache   cache[DagRunTask, DagRunTaskState]
	Config      taskSchedulerConfig
}

type taskSchedulerConfig struct {
	MaxConcurrentDagRuns      int
	HeartbeatMs               int
	CheckDependenciesStatusMs int
}

func defaultTaskSchedulerConfig() taskSchedulerConfig {
	return taskSchedulerConfig{
		MaxConcurrentDagRuns:      1000,
		HeartbeatMs:               1,
		CheckDependenciesStatusMs: 10,
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
			slog.Error("Error while scheduling new tasks", "dagId",
				string(err.DagId), "execTs", err.ExecTs, "err", err.Err)
		default:
		}
		if ts.DagRunQueue.Size() == 0 {
			// DagRunQueue is empty, we wait for a bit and then we'll try again
			time.Sleep(time.Duration(ts.Config.HeartbeatMs) * time.Millisecond)
			continue
		}
		/*dagrun*/ _, err := ts.DagRunQueue.Pop()
		if err == ds.ErrQueueIsEmpty {
			continue
		}
		if err != nil {
			// TODO: should we do anything else? Probably not, because item
			// should be probably still on the queue
			slog.Error("Error while getting dag run from the queue", "err", err)
			continue
		}
		// TODO(dskrzypiec): Think about the context. At least we need to add
		// timeout for overall DAG run timeout. Start scheduling new DAG run in
		// a separate goroutine.
		// TODO(dskrzypiec): uncomment when logging will be fixed
		//go ts.scheduleDagTasks(context.TODO(), dagrun, ts.TaskQueue,
		//	taskSchedulerErrors)
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
	slog.Debug("Start scheduling tasks", "dagId", dagId, "execTs",
		dagrun.AtTime)
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
		slog.Warn("DAG has no tasks, there is nothig to schedule", "dagId",
			dagrun.DagId)
		return
	}

	taskParents := dag.TaskParents()
	err := ts.walkAndSchedule(ctx, dagrun, dag.Root, taskParents)
	if err != nil {
		// TODO: what now?
	}

	// Step 4: Update dagrun state to finished
}

func (ts *taskScheduler) walkAndSchedule(
	ctx context.Context,
	dagrun DagRun,
	node *dag.Node,
	taskParents map[string][]string,
) error {
	taskId := node.Task.Id()

	for {
		select {
		case <-ctx.Done():
			// TODO: Handle cancelation
			slog.Error("Context canceled while walkAndSchedule", "err",
				ctx.Err())
			return ctx.Err()
		default:
		}

		canSchedule := ts.checkIfCanBeScheduled(dagrun, taskId, taskParents)
		if canSchedule {
			ts.scheduleSingleTask(dagrun, taskId)
			break
		}
		time.Sleep(time.Duration(ts.Config.CheckDependenciesStatusMs) * time.Millisecond)
	}

	for _, child := range node.Children {
		go ts.walkAndSchedule(ctx, dagrun, child, taskParents) // TODO: This won't work at the moment
	}

	return nil
}

// Schedules single task. That means putting metadata on the queue, updating
// cache, etc... TODO
func (ts *taskScheduler) scheduleSingleTask(dagrun DagRun, taskId string) {
	drt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	putErr := ts.TaskQueue.Put(drt)
	if putErr != nil {
		// TODO: handle if queue is full
	}
}

// Checks if dependecies (parent tasks) are done and we can proceed.
func (ts *taskScheduler) checkIfCanBeScheduled(
	dagrun DagRun,
	taskId string,
	taskParents map[string][]string,
) bool {
	parents, exists := taskParents[taskId]
	if !exists {
		slog.Error("Task doesn not exist in parents map", "taskId", taskId)
		return false
	}
	if len(parents) == 0 {
		// no parents, no dependecies
		return true
	}

	for _, parentTaskId := range parents {
		key := DagRunTask{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: parentTaskId,
		}
		statusFromCache, exists := ts.TaskCache.Get(key)
		if exists && !statusFromCache.Status.CanProceed() {
			return false
		}
		if exists && statusFromCache.Status.CanProceed() {
			continue
		}
		// If there is no entry in the cache, we need to query database
		slog.Debug("There is no entry in TaskCache, need to query database",
			"dagId", dagrun.DagId, "execTs", dagrun.AtTime, "taskId", taskId)
		ctx := context.TODO()
		dagruntask, err := ts.DbClient.ReadDagRunTask(
			ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime),
			parentTaskId,
		)
		if err != nil {
			// No need to handle it, this function will be retried.
			// TODO!!!! log.Error().Err(err).Msg("Cannot read DagRunTask status from DB")
			return false
		}
		drtStatus, sErr := stringToDagRunTaskStatus(dagruntask.Status)
		if sErr != nil {
			slog.Error("Cannot convert string to DagRunTaskStatus", "err", sErr)
			return false
		}
		if !drtStatus.CanProceed() {
			return false
		}
	}
	return true
}

type DagRunTaskStatus int

const (
	// TODO: more states
	Scheduled DagRunTaskStatus = iota
	Running
	Failed
	Success
)

func (s DagRunTaskStatus) String() string {
	return [...]string{
		"SCHEDULED",
		"RUNNING",
		"FAILED",
		"SUCCESS",
	}[s]
}

func (s DagRunTaskStatus) CanProceed() bool {
	// TODO: more states
	return s == Success
}

func stringToDagRunTaskStatus(s string) (DagRunTaskStatus, error) {
	states := map[string]DagRunTaskStatus{
		"SCHEDULED": Scheduled,
		"RUNNING":   Running,
		"FAILED":    Failed,
		"SUCCESS":   Success,
	}
	if status, ok := states[s]; ok {
		return status, nil
	}
	return 0, fmt.Errorf("invalid DagRunTaskStatus: %s", s)
}
