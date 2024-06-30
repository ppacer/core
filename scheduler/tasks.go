// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

type DagRunTask struct {
	DagId  dag.Id    `json:"dagId"`
	AtTime time.Time `json:"execTs"`
	TaskId string    `json:"taskId"`
}

type DagRunTaskState struct {
	Status         dag.TaskStatus
	StatusUpdateTs time.Time
}

// TaskScheduler is responsible for scheduling tasks for a single DagRun. When
// new DagRun is scheduled (by DagRunWatcher) it should appear on DagRunQueue,
// then TaskScheduler pick it up and start scheduling DAG tasks for that DagRun
// in a separate goroutine.
//
// If you use Scheduler, you probably don't need to use this object directly.
type TaskScheduler struct {
	DagRegistry        dag.Registry
	DbClient           *db.Client
	DagRunQueue        ds.Queue[DagRun]
	TaskQueue          ds.Queue[DagRunTask]
	TaskCache          ds.Cache[DagRunTask, DagRunTaskState]
	Config             TaskSchedulerConfig
	Logger             *slog.Logger
	Notifier           notify.Sender
	SchedulerStateFunc GetStateFunc
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
			ts.Logger.Error("Error while scheduling new tasks", "dagId",
				string(err.DagId), "execTs", err.ExecTs, "err", err.Err)
		default:
		}
		if ts.SchedulerStateFunc() == StateStopping {
			ts.Logger.Warn("Scheduler is stopping. TaskScheduler will not schedule other tasks")
			ts.Logger.Warn("Waiting for currently running tasks...")
			ts.waitForRunningTasks(1*time.Second, 1*time.Minute)
			return
		}
		if ts.DagRunQueue.Size() == 0 {
			// DagRunQueue is empty, we wait for a bit and then we'll try again
			time.Sleep(ts.Config.Heartbeat)
			continue
		}
		dagrun, err := ts.DagRunQueue.Pop()
		if err == ds.ErrQueueIsEmpty {
			continue
		}
		if err != nil {
			// TODO: should we do anything else? Probably not, because item
			// should be probably still on the queue
			ts.Logger.Error("Error while getting dag run from the queue", "err",
				err)
			continue
		}
		// TODO(dskrzypiec): Think about the context. At least we need to add
		// timeout for overall DAG run timeout. Start scheduling new DAG run in
		// a separate goroutine.
		d, existInRegistry := dags[dagrun.DagId]
		if !existInRegistry {
			ts.Logger.Error("TaskScheduler got DAG run for DAG which is not in given registry",
				"dagId", string(dagrun.DagId))
			continue
		}
		go ts.scheduleDagTasks(context.TODO(), d, dagrun, taskSchedulerErrors)
	}
}

// UpsertTaskStatus inserts or updates given DAG run task status. That includes
// caches, queues, database and every place that needs to be included regarding
// task status update.
func (ts *TaskScheduler) UpsertTaskStatus(ctx context.Context, drt DagRunTask, status dag.TaskStatus, taskErrStr *string) error {
	ts.Logger.Info("Start upserting dag run task status", "dagruntask", drt,
		"status", status.String())

	// Insert/update info in the cache
	drts := DagRunTaskState{Status: status, StatusUpdateTs: time.Now()}
	drtsCache, entryExists := ts.TaskCache.Get(drt)
	if !entryExists || (entryExists && drtsCache.Status != status) {
		ts.TaskCache.Put(drt, drts)
	}

	// Insert/update info in the database
	dagIdStr := string(drt.DagId)
	execTs := timeutils.ToString(drt.AtTime)
	drtDb, getErr := ts.DbClient.ReadDagRunTask(ctx, dagIdStr, execTs, drt.TaskId)
	switch getErr {
	case sql.ErrNoRows:
		ts.Logger.Info("Inserting new dag run task", "dagId", dagIdStr, "execTs",
			execTs, "taskId", drt.TaskId)
		iErr := ts.DbClient.InsertDagRunTask(
			ctx, dagIdStr, execTs, drt.TaskId, status.String(),
		)
		if iErr != nil {
			return iErr
		}
	case nil:
		ts.Logger.Info("Given dag run task exists in database", "dagruntask",
			drtDb)
		if drtDb.Status != status.String() {
			ts.Logger.Info("Updating dag run task status", "currentStatus",
				drtDb.Status, "newStatus", status.String())
			dbUpdateErr := ts.DbClient.UpdateDagRunTaskStatus(
				ctx, dagIdStr, execTs, drt.TaskId, status.String(),
			)
			if dbUpdateErr != nil {
				return dbUpdateErr
			}
		}
	default:
		ts.Logger.Error("Could not read from dagruntasks", "dagruntask", drt,
			"status", status.String())
		return getErr
	}
	if status == dag.TaskFailed {
		var taskErr error = nil
		if taskErrStr == nil {
			ts.Logger.Error("Got failed task with empty task error",
				"dagruntask", drt, "status", status.String())
		} else {
			taskErr = fmt.Errorf("%s", *taskErrStr)
		}
		aofErr := ts.actOnFailedTask(drt, taskErr)
		if aofErr != nil {
			// TODO(dskrzypiec): we probably want to retry this in case of
			// errors.
			ts.Logger.Error("error while running post-hooks on failed task",
				"dagruntask", drt, "err", aofErr)
		}
	}
	return nil
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
	dagId := string(dagrun.DagId)
	ts.Logger.Debug("Start scheduling tasks", "dagId", dagId, "execTs",
		dagrun.AtTime)
	execTs := timeutils.ToString(dagrun.AtTime)

	// Update dagrun state to running
	stateUpdateErr := ts.DbClient.UpdateDagRunStatusByExecTs(
		ctx, dagId, execTs, dag.RunRunning.String(),
	)
	if stateUpdateErr != nil {
		// just send error, we don't want to stop scheduling because of dagrun
		// status failure
		sendTaskSchedulerErr(errorsChan, dagrun, stateUpdateErr)
	}

	if d.Root == nil {
		ts.Logger.Warn("DAG has no tasks, there is nothig to schedule", "dagId",
			dagrun.DagId)
		// Update dagrun state to finished
		stateUpdateErr = ts.DbClient.UpdateDagRunStatusByExecTs(
			ctx, dagId, execTs, dag.RunSuccess.String(),
		)
		if stateUpdateErr != nil {
			// TODO(dskrzypiec): should we add retries? Probably...
			sendTaskSchedulerErr(errorsChan, dagrun, stateUpdateErr)
		}
		return
	}

	defer ts.cleanTaskCache(dagrun, d.Flatten())
	sharedState := newDagRunSharedState(d.TaskParents())
	var wg sync.WaitGroup
	wg.Add(1)
	ts.walkAndSchedule(ctx, dagrun, d.Root, sharedState, &wg)
	wg.Wait()

	// Check whenever any task has failed and if so, then mark downstream tasks
	// with status UPSTREAM_FAILED.
	ts.checkFailsAndMarkDownstream(ctx, dagrun, d.Root, sharedState)

	if ts.SchedulerStateFunc() == StateStopping {
		return
	}

	// At this point all tasks has been scheduled, but not necessarily done.
	tasks := d.Flatten()
	for !ts.allTasksAreDone(dagrun, tasks, sharedState) {
		time.Sleep(ts.Config.Heartbeat)
	}

	// Update dagrun state to finished
	stateUpdateErr = ts.DbClient.UpdateDagRunStatusByExecTs(
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
			runningTasks, err := ts.DbClient.RunningTasksNum(ctx)
			if err != nil {
				ts.Logger.Error("Error while checking DAG run running tasks",
					"err", err)
			}
			if runningTasks == 0 {
				return
			}
		case <-timeoutChan:
			ts.Logger.Error("Time out! No longer waiting for running tasks before quiting scheduler")
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
	checkDelay := ts.Config.CheckDependenciesStatusWait
	taskId := node.Task.Id()
	ts.Logger.Info("Start walkAndSchedule", "dagrun", dagrun, "taskId", taskId)

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
			ts.Logger.Error("Context canceled while walkAndSchedule", "dagrun",
				dagrun, "taskId", node.Task.Id(), "err", ctx.Err())
			// TODO: in this case we need to stop further scheduling and set
			// appropriate status
		default:
		}
		if alreadyFinished || ts.SchedulerStateFunc() == StateStopping {
			break
		}

		canSchedule, parentsStatus := ts.checkIfCanBeScheduled(
			dagrun, taskId, sharedState.TasksParents,
		)
		if parentsStatus == dag.TaskFailed {
			msg := "At least one parent of the task has failed. Will not proceed."
			ts.Logger.Warn(msg, "dagrun", dagrun, "taskId", taskId)
			return
		}
		if canSchedule {
			ts.scheduleSingleTask(dagrun, taskId)
			break
		}
		time.Sleep(checkDelay)
	}

	for _, child := range node.Children {
		drt := DagRunTask{dagrun.DagId, dagrun.AtTime, child.Task.Id()}
		_, alreadyStarted := sharedState.AlreadyMarkedTasks.Get(drt)
		if !alreadyStarted {
			sharedState.AlreadyMarkedTasks.Add(drt, struct{}{})
			wg.Add(1)
			go ts.walkAndSchedule(ctx, dagrun, child, sharedState, wg)
		}
	}
}

// Schedules single task. That means putting metadata on the queue, updating
// cache, etc... TODO
func (ts *TaskScheduler) scheduleSingleTask(dagrun DagRun, taskId string) {
	ts.Logger.Info("Start scheduling new dag run task", "dagrun", dagrun,
		"taskId", taskId)
	drt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // TODO: config
	defer cancel()
	ds.PutContext(ctx, ts.TaskQueue, drt)
	usErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskScheduled, nil)
	if usErr != nil {
		ts.Logger.Error("Cannot update dag run task status", "dagruntask", drt,
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
		ts.Logger.Error("Dag run is done but there is no cache entry and DB "+
			"entry for this dag run task. This is highly unexpected", "dagrun",
			dagrun, "taskId", node.Task.Id())
	}
	if err != nil && err != sql.ErrNoRows {
		ts.Logger.Error("Could no get dag run task status. Dag run is done. "+
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
		drt := DagRunTask{dagrun.DagId, dagrun.AtTime, task.Node.Task.Id()}
		uErr := ts.UpsertTaskStatus(ctx, drt, status, nil)
		if uErr != nil {
			ts.Logger.Error("UpsertTaskStatus failed during markDownstreamTasks",
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
		ts.Logger.Error("Canot get DAG run task status",
			"dagrun", dagrun, "taskId", taskId, "err", err.Error())
		return false, dag.TaskNoStatus
	}
	return status.IsTerminal(), status
}

// Checks if dependecies (parent tasks) are done and we can proceed.
func (ts *TaskScheduler) checkIfCanBeScheduled(
	dagrun DagRun,
	taskId string,
	tasksParents *ds.AsyncMap[string, []string],
) (bool, dag.TaskStatus) {
	parents, exists := tasksParents.Get(taskId)
	if !exists {
		ts.Logger.Error("Task does not exist in parents map", "taskId", taskId)
		return false, dag.TaskRunning
	}
	if len(parents) == 0 {
		// no parents, no dependecies
		return true, dag.TaskSuccess
	}

	for _, parentTaskId := range parents {
		key := DagRunTask{
			DagId:  dagrun.DagId,
			AtTime: dagrun.AtTime,
			TaskId: parentTaskId,
		}
		isParentTaskDone, status := ts.isTaskDone(dagrun, key)
		if status == dag.TaskFailed {
			return false, dag.TaskFailed
		}
		if !isParentTaskDone {
			return false, dag.TaskRunning
		}
	}
	return true, dag.TaskSuccess
}

// This routine is called for failed DAG run tasks.
func (ts *TaskScheduler) actOnFailedTask(drt DagRunTask, execErr error) error {
	// TODO(dskrzypiec): should we lift this logic to the separated type?
	dag, exists := ts.DagRegistry[drt.DagId]
	if !exists {
		return fmt.Errorf("dag [%s] does not exist in the registry",
			string(drt.DagId))
	}
	drtNode, nErr := dag.GetNode(drt.TaskId)
	if nErr != nil {
		return nErr
	}

	if drtNode.Config.SendAlertOnFailure {
		ctx := context.TODO()
		msg := notify.MsgData{
			DagId:        string(drt.DagId),
			ExecTs:       timeutils.ToString(drt.AtTime),
			TaskId:       &drt.TaskId,
			TaskRunError: execErr,
		}
		ts.Notifier.Send(ctx, drtNode.Config.AlertOnFailureTemplate, msg)
	}

	return nil
}

// Check if given parent task in given dag run is completed, to determine if
// DAG can proceed forward. It checks cache first and if there is no info there
// it reaches the database, to check the status.
func (ts *TaskScheduler) isTaskDone(
	dagrun DagRun, parent DagRunTask,
) (bool, dag.TaskStatus) {
	statusFromCache, exists := ts.TaskCache.Get(parent)
	if exists && !statusFromCache.Status.CanProceed() {
		return false, statusFromCache.Status
	}
	if exists && statusFromCache.Status.CanProceed() {
		return true, statusFromCache.Status
	}
	// If there is no entry in the cache, we need to query database
	ts.Logger.Warn("There is no entry in TaskCache, need to query database",
		"dagId", dagrun.DagId, "execTs", dagrun.AtTime, "taskId", parent.TaskId)
	ctx := context.TODO()
	dagruntask, err := ts.DbClient.ReadDagRunTask(
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
		ts.Logger.Error("Cannot read DagRunTask status from DB", "err", err)
		return false, dag.TaskRunning
	}
	drtStatus, sErr := dag.ParseTaskStatus(dagruntask.Status)
	if sErr != nil {
		ts.Logger.Error("Cannot convert string to DagRunTaskStatus", "status",
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
	drt := DagRunTask{dagrun.DagId, dagrun.AtTime, taskId}
	drts, exists := ts.TaskCache.Get(drt)
	if exists {
		return drts.Status, nil
	}
	ctx := context.TODO()
	drts, dbErr := ts.getDagRunTaskStatusFromDb(ctx, drt)
	if dbErr == sql.ErrNoRows {
		return dag.TaskNoStatus, dbErr
	}
	if dbErr != nil {
		ts.Logger.Error(
			"Dag run task does not exist in the cache and cannot get it "+
				"from database", "dagruntask", drt, "err", dbErr.Error())
		return dag.TaskNoStatus, dbErr
	}
	ts.TaskCache.Put(drt, drts)
	return drts.Status, nil
}

// Gets dag run task status from the database.
func (ts *TaskScheduler) getDagRunTaskStatusFromDb(
	ctx context.Context, drt DagRunTask,
) (DagRunTaskState, error) {
	dagruntask, err := ts.DbClient.ReadDagRunTask(
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
func (ts *TaskScheduler) cleanTaskCache(dagrun DagRun, tasks []dag.Task) {
	for _, task := range tasks {
		ts.TaskCache.Remove(
			DagRunTask{
				DagId:  dagrun.DagId,
				AtTime: dagrun.AtTime,
				TaskId: task.Id(),
			},
		)
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
