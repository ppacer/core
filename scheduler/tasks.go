// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

// DagRunTask represents an identifier for a single DAG run task which shall be
// scheduled.
type DagRunTask struct {
	DagId  dag.Id    `json:"dagId"`
	AtTime time.Time `json:"execTs"`
	TaskId string    `json:"taskId"`
	Retry  int       `json:"retry"`
}

// NextRetry creates new instance of DagRunTask for the next retry.
func (drt DagRunTask) NextRetry() DagRunTask {
	return DagRunTask{
		DagId:  drt.DagId,
		AtTime: drt.AtTime,
		TaskId: drt.TaskId,
		Retry:  drt.Retry + 1,
	}
}

// Base casts DagRunTask into DRTBase type.
func (drt DagRunTask) Base() DRTBase {
	return DRTBase{
		DagId:  drt.DagId,
		AtTime: drt.AtTime,
		TaskId: drt.TaskId,
	}
}

// DagRunTaskState represents DagRunTask state.
type DagRunTaskState struct {
	Status         dag.TaskStatus
	StatusUpdateTs time.Time
}

// DRTBase represents base information regarding identification a DAG run task.
// It meant to be used in caching where we care about the latest status for the
// DAG run task and doesn't need to store info for all retries.
type DRTBase struct {
	DagId  dag.Id
	AtTime time.Time
	TaskId string
}

// TaskScheduler is responsible for scheduling tasks for a single DagRun. When
// new DagRun is scheduled (by DagRunWatcher) it should appear on DagRunQueue,
// then TaskScheduler pick it up and start scheduling DAG tasks for that DagRun
// in a separate goroutine.
//
// If you use Scheduler, you probably don't need to use this object directly.
type TaskScheduler struct {
	dagRegistry        dag.Registry
	dbClient           *db.Client
	dagRunQueue        ds.Queue[DagRun]
	taskQueue          ds.Queue[DagRunTask]
	taskCache          ds.Cache[DRTBase, DagRunTaskState]
	config             TaskSchedulerConfig
	logger             *slog.Logger
	notifier           notify.Sender
	goroutineCount     *int64
	schedulerStateFunc GetStateFunc

	failedTaskManager   *failedTaskManager
	taskRetriesExecuted *ds.AsyncMap[DRTBase, int]
}

// NewTaskScheduler initialize new instance of *TaskScheduler.
func NewTaskScheduler(
	dags dag.Registry, db *db.Client, queues Queues,
	cache ds.Cache[DRTBase, DagRunTaskState], config TaskSchedulerConfig,
	logger *slog.Logger, notifier notify.Sender, goroutineCount *int64,
	schedulerStateFunc GetStateFunc,
) *TaskScheduler {
	if logger == nil {
		opts := slog.HandlerOptions{Level: slog.LevelInfo}
		logger = slog.New(slog.NewTextHandler(os.Stdout, &opts))
	}

	return &TaskScheduler{
		dagRegistry:        dags,
		dbClient:           db,
		dagRunQueue:        queues.DagRuns,
		taskQueue:          queues.DagRunTasks,
		taskCache:          cache,
		config:             config,
		logger:             logger,
		notifier:           notifier,
		goroutineCount:     goroutineCount,
		schedulerStateFunc: schedulerStateFunc,

		failedTaskManager: newFailedTaskManager(
			dags, db, queues.DagRunTasks, cache, config, logger,
		),

		taskRetriesExecuted: ds.NewAsyncMap[DRTBase, int](),
	}
}

type taskSchedulerError struct {
	DagId  dag.Id
	ExecTs time.Time
	Err    error
}

// Start starts TaskScheduler main loop. It check if there are new DagRuns on
// the DagRunQueue and if so, it spins up DAG tasks scheduling for that DagRun
// in a separate goroutine. If DagRunQueue is empty at the moment, then main
// loop waits Config.HeartbeatMs milliseconds before the next try.
func (ts *TaskScheduler) Start(dags dag.Registry) {
	taskSchedulerErrors := make(chan taskSchedulerError, 100)
	for {
		select {
		case err := <-taskSchedulerErrors:
			// TODO(dskrzypiec): What do we want to do with those errors?
			ts.logger.Error("Error while scheduling new tasks", "dagId",
				string(err.DagId), "execTs", err.ExecTs, "err", err.Err)
		default:
		}
		if ts.schedulerStateFunc() == StateStopping {
			ts.logger.Warn("Scheduler is stopping. TaskScheduler will not schedule other tasks")
			ts.logger.Warn("Waiting for currently running tasks...")
			ts.waitForRunningTasks(1*time.Second, 1*time.Minute)
			return
		}
		if ts.dagRunQueue.Size() == 0 {
			// DagRunQueue is empty, we wait for a bit and then we'll try again
			time.Sleep(ts.config.Heartbeat)
			continue
		}
		dagrun, err := ts.dagRunQueue.Pop()
		if err == ds.ErrQueueIsEmpty {
			continue
		}
		if err != nil {
			// TODO: should we do anything else? Probably not, because item
			// should be probably still on the queue
			ts.logger.Error("Error while getting dag run from the queue", "err",
				err)
			continue
		}
		// TODO(dskrzypiec): Think about the context. At least we need to add
		// timeout for overall DAG run timeout. Start scheduling new DAG run in
		// a separate goroutine.
		d, existInRegistry := dags[dagrun.DagId]
		if !existInRegistry {
			ts.logger.Error("TaskScheduler got DAG run for DAG which is not in given registry",
				"dagId", string(dagrun.DagId))
			continue
		}

		ts.waitIfCannotSpawnNewGoroutine(
			fmt.Sprintf("scheduling dagrun=%v", dagrun),
		)
		atomic.AddInt64(ts.goroutineCount, 1)
		go ts.scheduleDagTasks(context.TODO(), d, dagrun, taskSchedulerErrors)
	}
}

// UpsertTaskStatus inserts or updates given DAG run task status. That includes
// caches, queues, database and every place that needs to be included regarding
// task status update.
func (ts *TaskScheduler) UpsertTaskStatus(ctx context.Context, drt DagRunTask, status dag.TaskStatus, taskErrStr *string) error {
	ts.logger.Info("Start upserting dag run task status", "dagruntask", drt,
		"status", status.String())

	if status.IsTerminal() {
		ts.taskRetriesExecuted.Add(drt.Base(), drt.Retry)
	}
	shouldBeRetried, delay, rErr := ts.failedTaskManager.ShouldBeRetried(
		drt, status,
	)
	if rErr != nil {
		return rErr
	}
	if shouldBeRetried {
		status = dag.TaskFailed
	}

	// Insert/update task info in cache and database
	ts.upsertTaskStatusCache(drt, status)
	dbErr := ts.upsertTaskStatusDb(ctx, drt, status)
	if dbErr != nil {
		return dbErr
	}

	if shouldBeRetried {
		go func() {
			// Let's remember that when we would restart Scheduler between
			// particular DAG run task retries we might wait somewhere in
			// [delay, 2 * delay) interval. For now it's fine, but let's make
			// it resilient to Scheduler restarts.
			time.Sleep(delay)
			ts.scheduleRetry(drt)
		}()
	}

	if status != dag.TaskFailed {
		// no need to do anything else
		return nil
	}

	// we have a failed task
	alertsErr := ts.failedTaskManager.CheckAndSendAlerts(
		drt, status, shouldBeRetried, taskErrStr, ts.notifier,
	)
	if alertsErr != nil {
		ts.logger.Error("Cannot send external alerts on task retry or failure",
			"dagruntask", drt, "status", status, "err", alertsErr,
		)
	}
	return nil
}

//  Insert/update DAG run task info in the cache.
func (ts *TaskScheduler) upsertTaskStatusCache(drt DagRunTask, status dag.TaskStatus) {
	drts := DagRunTaskState{Status: status, StatusUpdateTs: timeutils.Now()}
	drtsCache, entryExists := ts.taskCache.Get(drt.Base())
	if !entryExists || (entryExists && drtsCache.Status != status) {
		ts.taskCache.Put(drt.Base(), drts)
	}
}

// Insert/update info in the database
func (ts *TaskScheduler) upsertTaskStatusDb(
	ctx context.Context, drt DagRunTask, status dag.TaskStatus,
) error {
	dagIdStr := string(drt.DagId)
	execTs := timeutils.ToString(drt.AtTime)
	drtDb, getErr := ts.dbClient.ReadDagRunTask(
		ctx, dagIdStr, execTs, drt.TaskId, drt.Retry,
	)
	switch getErr {
	case sql.ErrNoRows:
		ts.logger.Info("Inserting new dag run task", "dagId", dagIdStr,
			"execTs", execTs, "taskId", drt.TaskId)
		iErr := ts.dbClient.InsertDagRunTask(
			ctx, dagIdStr, execTs, drt.TaskId, drt.Retry, status.String(),
		)
		if iErr != nil {
			return iErr
		}
	case nil:
		ts.logger.Info("Given dag run task exists in database", "dagruntask",
			drtDb)
		if drtDb.Status != status.String() {
			ts.logger.Info("Updating dag run task status", "currentStatus",
				drtDb.Status, "newStatus", status.String())
			dbUpdateErr := ts.dbClient.UpdateDagRunTaskStatus(
				ctx, dagIdStr, execTs, drt.TaskId, drt.Retry, status.String(),
			)
			if dbUpdateErr != nil {
				return dbUpdateErr
			}
		}
	default:
		ts.logger.Error("Could not read from dagruntasks", "dagruntask", drt,
			"status", status.String())
		return getErr
	}
	return nil
}

// Schedules new retry for given DAG run task.
func (ts *TaskScheduler) scheduleRetry(drt DagRunTask) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, ts.config.PutOnTaskQueueTimeout)
	defer cancel()
	ds.PutContext(ctx, ts.taskQueue, drt.NextRetry())
}

// Function scheduleDagTasks is responsible for scheduling tasks of single DAG
// run. Each call to this function by taskScheduler is fire up in separate
// goroutine.
func (ts *TaskScheduler) scheduleDagTasks(
	ctx context.Context,
	d dag.Dag,
	dagrun DagRun,
	errorsChan chan taskSchedulerError,
) {
	defer atomic.AddInt64(ts.goroutineCount, -1)
	dagId := string(dagrun.DagId)
	ts.logger.Debug("Start scheduling tasks", "dagId", dagId, "execTs",
		dagrun.AtTime)
	execTs := timeutils.ToString(dagrun.AtTime)

	// Update dagrun state to running
	stateUpdateErr := ts.dbClient.UpdateDagRunStatusByExecTs(
		ctx, dagId, execTs, dag.RunRunning.String(),
	)
	if stateUpdateErr != nil {
		// just send error, we don't want to stop scheduling because of dagrun
		// status failure
		sendTaskSchedulerErr(errorsChan, dagrun, stateUpdateErr)
	}

	if d.Root == nil {
		ts.logger.Warn("DAG has no tasks, there is nothig to schedule", "dagId",
			dagrun.DagId)
		// Update dagrun state to finished
		stateUpdateErr = ts.dbClient.UpdateDagRunStatusByExecTs(
			ctx, dagId, execTs, dag.RunSuccess.String(),
		)
		if stateUpdateErr != nil {
			// TODO(dskrzypiec): should we add retries? Probably...
			sendTaskSchedulerErr(errorsChan, dagrun, stateUpdateErr)
		}
		return
	}

	defer ts.cleanTaskCache(dagrun, d.FlattenNodes())
	sharedState := newDagRunSharedState(d.TaskParents())
	var wg sync.WaitGroup
	wg.Add(1)
	ts.walkAndSchedule(ctx, dagrun, d.Root, sharedState, &wg)
	wg.Wait()

	// Check whenever any task has failed and if so, then mark downstream tasks
	// with status UPSTREAM_FAILED.
	ts.checkFailsAndMarkDownstream(ctx, dagrun, d.Root, sharedState)

	if ts.schedulerStateFunc() == StateStopping {
		return
	}

	// At this point all tasks has been scheduled, but not necessarily done.
	tasks := d.Flatten()
	for !ts.allTasksAreDone(dagrun, tasks, sharedState) {
		time.Sleep(ts.config.Heartbeat)
	}

	// Update dagrun state to finished
	stateUpdateErr = ts.dbClient.UpdateDagRunStatusByExecTs(
		ctx, dagId, execTs, (*sharedState.DagRunStatus).String(),
	)
	if stateUpdateErr != nil {
		// TODO(dskrzypiec): should we add retries? Probably...
		sendTaskSchedulerErr(errorsChan, dagrun, stateUpdateErr)
	}
}

// Waits for currently running tasks or timeouts.
func (ts *TaskScheduler) waitForRunningTasks(pollInterval, timeout time.Duration) {
	ctx := context.TODO()
	ticker := time.NewTicker(pollInterval)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			runningTasks, err := ts.dbClient.RunningTasksNum(ctx)
			if err != nil {
				ts.logger.Error("Error while checking DAG run running tasks",
					"err", err)
			}
			if runningTasks == 0 {
				ts.logger.Info("All running tasks are done")
				return
			}
		case <-timeoutChan:
			ts.logger.Error("Time out! No longer waiting for running tasks before quiting scheduler")
			return
		}
	}
}

// Type dagRunSharedState represents shared state for goroutines spinned within
// the same dag run for scheduling multiple tasks. Those goroutines need to
// share metadata. One example is when a task has multiple parents, we want to
// create only one new goroutine which would await for scheduling that task.
// Similar case is when we propagate upstream failure down the tree.
type dagRunSharedState struct {
	// Map of dag run tasks that has been already started to be scheduled in a
	// separate goroutine. Having this helps, to avoid firing up few goroutines
	// for the same dag run task. For example:
	// n21
	//     \
	// n22 - n3
	//     /
	// n23
	// We don't need to start 3 separate goroutines for the same task, only
	// because it has 3 parents.
	AlreadyMarkedTasks *ds.AsyncMap[DagRunTask, any]

	// TODO
	AlreadyMarkedForUpstreamFailure *ds.AsyncMap[DagRunTask, any]

	// Map of taskId -> []{ parent task ids } for the DAG.
	TasksParents *ds.AsyncMap[string, []string]

	// Overall dag run status
	sync.Mutex
	DagRunStatus *dag.RunStatus
}

func newDagRunSharedState(taskParents map[string][]string) *dagRunSharedState {
	dagrunStatus := dag.RunSuccess
	return &dagRunSharedState{
		AlreadyMarkedTasks:              ds.NewAsyncMap[DagRunTask, any](),
		AlreadyMarkedForUpstreamFailure: ds.NewAsyncMap[DagRunTask, any](),
		TasksParents:                    ds.NewAsyncMapFromMap(taskParents),
		DagRunStatus:                    &dagrunStatus,
	}
}

// WalkAndSchedule wait for node.Task to be ready for scheduling, then
// schedules it and then goes recursively for that node children.
// TODO: More details when it become stable.
func (ts *TaskScheduler) walkAndSchedule(
	ctx context.Context,
	dagrun DagRun,
	node *dag.Node,
	sharedState *dagRunSharedState,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer atomic.AddInt64(ts.goroutineCount, -1)
	checkDelay := ts.config.CheckDependenciesStatusWait
	taskId := node.Task.Id()
	ts.logger.Info("Start walkAndSchedule", "dagrun", dagrun, "taskId", taskId)

	// In case when scheduler has been restarted given task might been
	// already completed from the previous unfinished DAG run.
	alreadyFinished, status := ts.checkIfAlreadyFinished(dagrun, taskId)
	if alreadyFinished {
		if status != dag.TaskSuccess {
			ts.scheduleSingleTask(dagrun, taskId)
		}
	}

	for {
		select {
		case <-ctx.Done():
			// TODO: What to do with errors in here? Probably we should have a
			// queue for retries on DagRunTasks level.
			ts.logger.Error("Context canceled while walkAndSchedule", "dagrun",
				dagrun, "taskId", node.Task.Id(), "err", ctx.Err())
			// TODO: in this case we need to stop further scheduling and set
			// appropriate status
		default:
		}
		if alreadyFinished || ts.schedulerStateFunc() == StateStopping {
			break
		}

		canSchedule, parentsStatus := ts.checkIfCanBeScheduled(
			dagrun, taskId, sharedState.TasksParents,
		)
		if parentsStatus == dag.TaskFailed {
			msg := "At least one parent of the task has failed. Will not proceed."
			ts.logger.Warn(msg, "dagrun", dagrun, "taskId", taskId)
			return
		}
		if canSchedule {
			ts.scheduleSingleTask(dagrun, taskId)
			break
		}
		time.Sleep(checkDelay)
	}

	for _, child := range node.Children {
		drt := DagRunTask{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: child.Task.Id(),
			Retry:  0,
		}
		_, alreadyStarted := sharedState.AlreadyMarkedTasks.Get(drt)
		if !alreadyStarted {
			sharedState.AlreadyMarkedTasks.Add(drt, struct{}{})
			wg.Add(1)

			ts.waitIfCannotSpawnNewGoroutine(
				fmt.Sprintf("scheduling walkAndSchedule for drt=%v", drt),
			)
			atomic.AddInt64(ts.goroutineCount, 1)
			go ts.walkAndSchedule(ctx, dagrun, child, sharedState, wg)
		}
	}
}

// Schedules single task. That means putting metadata on the queue, updating
// cache, etc... TODO
func (ts *TaskScheduler) scheduleSingleTask(dagrun DagRun, taskId string) {
	ts.logger.Info("Start scheduling new dag run task", "dagrun", dagrun,
		"taskId", taskId)
	drt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, ts.config.PutOnTaskQueueTimeout)
	defer cancel()
	ds.PutContext(ctx, ts.taskQueue, drt)
	usErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskScheduled, nil)
	if usErr != nil {
		ts.logger.Error("Cannot update dag run task status", "dagruntask", drt,
			"status", dag.TaskScheduled.String(), "err", usErr)
		// Consider putting those on the TaskToRetryQueue
		// This mechanism will be implemented under "task retries" story.
	}
}

// CheckFailsAndMarkDownstream performs DFS and if it finds a task in the tree
// which is in FAILED state it marks the dag run as FAILED and also marks all
// tasks in this sub-tree with status UPSTREAM_FAILED.
func (ts *TaskScheduler) checkFailsAndMarkDownstream(
	ctx context.Context,
	dagrun DagRun,
	node *dag.Node,
	sharedState *dagRunSharedState,
) {
	status, err := ts.getDagRunTaskStatus(dagrun, node.Task.Id())
	if err == sql.ErrNoRows {
		ts.logger.Error("Dag run is done but there is no cache entry and DB "+
			"entry for this dag run task. This is highly unexpected", "dagrun",
			dagrun, "taskId", node.Task.Id())
	}
	if err != nil && err != sql.ErrNoRows {
		ts.logger.Error("Could no get dag run task status. Dag run is done. "+
			"This is unexpected. This dag run task is treated as not failed",
			"dagrun", dagrun, "taskId", node.Task.Id())
	}
	if status != dag.TaskFailed {
		// Parent is not FAILED, we should continue DFS.
		for _, child := range node.Children {
			ts.checkFailsAndMarkDownstream(ctx, dagrun, child, sharedState)
		}
	} else {
		// Parent is in FAILED state - we should mark it's subtree with
		// UPSTREAM_FAILED status.
		sharedState.Lock()
		*sharedState.DagRunStatus = dag.RunFailed
		sharedState.Unlock()
		for _, child := range node.Children {
			ts.markDownstreamTasks(ctx, dagrun, child, dag.TaskUpstreamFailed)
		}
	}
}

// Mark downstream node tasks of given node (inclusive) with given status.
// Usually used to mark downstream tasks with status UPSTREAM_FAILED when one
// of parents fails.
func (ts *TaskScheduler) markDownstreamTasks(
	ctx context.Context, dagrun DagRun, node *dag.Node, status dag.TaskStatus,
) {
	for _, task := range node.Flatten() {
		drt := DagRunTask{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: task.Node.Task.Id(),
			Retry:  0,
		}
		uErr := ts.UpsertTaskStatus(ctx, drt, status, nil)
		if uErr != nil {
			ts.logger.Error("UpsertTaskStatus failed during markDownstreamTasks",
				"dagrun", dagrun, "taskId", node.Task.Id(), "status", status)
		}
	}
}

// Checks whenever given task in a DAG run has been already finished. This is
// very handy in case when the scheduler has been restarted and before the
// restart not all tasks in a DAG run has been completed. In this case we shall
// simply go through already finished tasks and continue to scheduling not yet
// done tasks.
func (ts *TaskScheduler) checkIfAlreadyFinished(
	dagrun DagRun, taskId string,
) (bool, dag.TaskStatus) {
	status, err := ts.getDagRunTaskStatus(dagrun, taskId)
	if err == sql.ErrNoRows {
		return false, dag.TaskNoStatus
	}
	if err != nil {
		ts.logger.Error("Canot get DAG run task status",
			"dagrun", dagrun, "taskId", taskId, "err", err.Error())
		return false, dag.TaskNoStatus
	}
	return status.IsTerminal(), status
}

// Checks if dependecies (parent tasks) are done and we can proceed.
func (ts *TaskScheduler) checkIfCanBeScheduled(
	dagrun DagRun, taskId string,
	tasksParents *ds.AsyncMap[string, []string],
) (bool, dag.TaskStatus) {
	parents, exists := tasksParents.Get(taskId)
	if !exists {
		ts.logger.Error("Task does not exist in parents map", "taskId", taskId)
		return false, dag.TaskRunning
	}
	if len(parents) == 0 {
		// no parents, no dependecies
		return true, dag.TaskSuccess
	}

	for _, parentTaskId := range parents {
		base := DRTBase{dagrun.DagId, dagrun.AtTime, parentTaskId}
		parentTaskRetry := 0
		if retry, exists := ts.taskRetriesExecuted.Get(base); exists {
			parentTaskRetry = retry
		}
		key := DagRunTask{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: parentTaskId,
			Retry:  parentTaskRetry,
		}
		isParentTaskDone, status := ts.isTaskDone(dagrun, key)
		if status == dag.TaskFailed {
			shouldBeRetried, _, resErr := ts.failedTaskManager.ShouldBeRetried(
				key, status,
			)
			if resErr != nil {
				ts.logger.Error("Cannot determine if task should be restarted",
					"dagruntask", key, "err", resErr)
			}
			if shouldBeRetried {
				return false, dag.TaskRestarting
			}
			return false, dag.TaskFailed
		}
		if !isParentTaskDone {
			return false, dag.TaskRunning
		}
	}
	return true, dag.TaskSuccess
}

// Check if given parent task in given dag run is completed, to determine if
// DAG can proceed forward. It checks cache first and if there is no info there
// it reaches the database, to check the status.
func (ts *TaskScheduler) isTaskDone(
	dagrun DagRun, parent DagRunTask,
) (bool, dag.TaskStatus) {
	statusFromCache, exists := ts.taskCache.Get(parent.Base())
	if exists && !statusFromCache.Status.CanProceed() {
		return false, statusFromCache.Status
	}
	if exists && statusFromCache.Status.CanProceed() {
		return true, statusFromCache.Status
	}
	// If there is no entry in the cache, we need to query database
	ctx := context.TODO()
	dagruntask, err := ts.dbClient.ReadDagRunTaskLatest(
		ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime),
		parent.TaskId,
	)
	if err == sql.ErrNoRows {
		// There is no entry in the cache and in the database, we are
		// probably a bit early - either task queue was full at the moment
		// or the other goroutine is a bit slower. Will try again.
		return false, dag.TaskRunning
	}
	if err != nil {
		// No need to handle it, this function will be retried.
		ts.logger.Error("Cannot read DagRunTask status from DB", "err", err)
		return false, dag.TaskRunning
	}
	drtStatus, sErr := dag.ParseTaskStatus(dagruntask.Status)
	if sErr != nil {
		ts.logger.Error("Cannot convert string to DagRunTaskStatus", "status",
			dagruntask.Status, "err", sErr)
		return false, dag.TaskRunning
	}
	if !drtStatus.CanProceed() {
		return false, drtStatus
	}
	return true, drtStatus
}

// Checks whenever all tasks within the dag run are in terminal states and thus
// dag run is finished.
func (ts *TaskScheduler) allTasksAreDone(
	dagrun DagRun, tasks []dag.Task, sharedState *dagRunSharedState,
) bool {
	for _, task := range tasks {
		status, err := ts.getDagRunTaskStatus(dagrun, task.Id())
		if err != nil {
			return false
		}
		if !status.IsTerminal() {
			return false
		}
		if status == dag.TaskFailed {
			// Almost all cases should be covered by
			// checkFailsAndMarkDownstream but in case when all tasks are
			// scheduled, then checkFailsAndMarkDownstream might be run before
			// all task (especially leafs) has been done. Those cases are
			// cought only in here. There is no need for marking downstream
			// tasks, because there is no downstream tasks. This can only
			// happen for leafs of the tree.
			sharedState.Lock()
			*sharedState.DagRunStatus = dag.RunFailed
			sharedState.Unlock()
		}
	}
	return true
}

// This methods gets dag run task status. It tries to check TaskCache first.
// When given dag run task is not there it tries to pull it from database. If
// there is also no entry in the database, then sql.ErrNoRows is returned.
func (ts *TaskScheduler) getDagRunTaskStatus(
	dagrun DagRun, taskId string,
) (dag.TaskStatus, error) {
	drt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	drts, exists := ts.taskCache.Get(drt.Base())
	if exists {
		return drts.Status, nil
	}
	ctx := context.TODO()
	drts, dbErr := ts.getDagRunTaskStatusFromDb(ctx, drt)
	if dbErr == sql.ErrNoRows {
		return dag.TaskNoStatus, dbErr
	}
	if dbErr != nil {
		ts.logger.Error(
			"Dag run task does not exist in the cache and cannot get it "+
				"from database", "dagruntask", drt, "err", dbErr.Error())
		return dag.TaskNoStatus, dbErr
	}
	ts.taskCache.Put(drt.Base(), drts)
	return drts.Status, nil
}

// Gets dag run task status from the database.
func (ts *TaskScheduler) getDagRunTaskStatusFromDb(
	ctx context.Context, drt DagRunTask,
) (DagRunTaskState, error) {
	dagruntask, err := ts.dbClient.ReadDagRunTaskLatest(
		ctx, string(drt.DagId), timeutils.ToString(drt.AtTime), drt.TaskId,
	)
	if err != nil {
		return DagRunTaskState{}, err
	}
	status, sErr := dag.ParseTaskStatus(dagruntask.Status)
	if sErr != nil {
		return DagRunTaskState{}, sErr
	}
	drts := DagRunTaskState{
		Status:         status,
		StatusUpdateTs: timeutils.FromStringMust(dagruntask.StatusUpdateTs),
	}
	return drts, nil
}

// Removes bulk of tasks for given dag run from the TaskCache.
func (ts *TaskScheduler) cleanTaskCache(dagrun DagRun, nodes []dag.NodeInfo) {
	for _, node := range nodes {
		drtBase := DRTBase{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: node.Node.Task.Id(),
		}
		ts.taskCache.Remove(drtBase)
		ts.taskRetriesExecuted.Delete(drtBase)
	}
}

// waitIfCannotSpawnNewGoroutine check whenever new goroutine could be started,
// based on maxGoroutineCount configuration. If that cannot be done, this
// method would block until number of goroutines is below the limit.
func (ts *TaskScheduler) waitIfCannotSpawnNewGoroutine(msg string) {
	prevTs := time.Now()
	for {
		if atomic.LoadInt64(ts.goroutineCount) < int64(ts.config.MaxGoroutineCount) {
			return
		}
		now := time.Now()
		if now.Sub(prevTs) > 30*time.Second {
			ts.logger.Warn("Cannot yet start new goroutine, Scheduler hit the limit.",
				"limit", ts.config.MaxGoroutineCount, "msg", msg)
			prevTs = now
		}
	}
}

func sendTaskSchedulerErr(
	errChan chan taskSchedulerError,
	dagrun DagRun,
	err error,
) {
	errChan <- taskSchedulerError{
		DagId:  dagrun.DagId,
		ExecTs: dagrun.AtTime,
		Err:    err,
	}
}
