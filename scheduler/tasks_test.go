// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

type EmptyTask struct {
	TaskId string
}

func (et EmptyTask) Id() string { return et.TaskId }

func (et EmptyTask) Execute(_ dag.TaskContext) error {
	fmt.Println(et.TaskId)
	return nil
}

func TestCheckIfCanBeScheduledFirstTask(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	start := dag.NewNode(EmptyTask{"start"})
	end := dag.NewNode(EmptyTask{"end"})
	start.Next(end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(start).Done()
	ts.dagRegistry.Add(d)
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	tasksParents := ds.NewAsyncMapFromMap(d.TaskParents())

	startShouldBeSched, _ := ts.checkIfCanBeScheduled(
		dagrun, "start", tasksParents,
	)
	if !startShouldBeSched {
		t.Error("Task <start> should be scheduled, but it's not")
	}

	endShouldBeSched, _ := ts.checkIfCanBeScheduled(
		dagrun, "end", tasksParents,
	)
	if endShouldBeSched {
		t.Error("Task <end> should not be scheduled in this case, but it is")
	}
}

func TestScheduleSingleTaskSimple(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	start := dag.NewNode(EmptyTask{"start"})
	end := dag.NewNode(EmptyTask{"end"})
	start.Next(end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_simple").AddSchedule(schedule).AddRoot(start).Done()
	ts.dagRegistry.Add(d)
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	taskId := "start"

	ts.scheduleSingleTask(dagrun, taskId)

	// Task should be on the TaskQeueu
	expectedDrt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	drt, popErr := ts.taskQueue.Pop()
	if popErr != nil {
		t.Errorf("Error while popping task from the queue: %s", popErr.Error())
	}
	if drt != expectedDrt {
		t.Errorf("Expected popped dag run task from queue to be %v, got %v",
			expectedDrt, drt)
	}

	// Task's status should be cached
	cacheStatus, existInCache := ts.taskCache.Get(drt.Base())
	if !existInCache {
		t.Errorf("Scheduled task %v does not exist in TaskCache", drt)
	}
	if cacheStatus.Status != dag.TaskScheduled {
		t.Errorf("Expected cached status of %v, to be %s, got %s",
			drt, dag.TaskScheduled.String(), cacheStatus.Status.String())
	}

	// Task should be inserted into the database
	ctx := context.Background()
	drtDb, dbErr := ts.dbClient.ReadDagRunTaskLatest(
		ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime), taskId,
	)
	if dbErr == sql.ErrNoRows {
		t.Errorf("There is no row in dagruntasks for %v", drt)
	}
	if drtDb.Status != dag.TaskScheduled.String() {
		t.Errorf("DagRunTask %v in the database has status: %s, but expected %s",
			drt, drtDb.Status, dag.TaskScheduled.String())
	}
}

func TestWalkAndScheduleOnTwoTasks(t *testing.T) {
	// Prepare scenario
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	start := dag.NewNode(EmptyTask{"start"})
	end := dag.NewNode(EmptyTask{"end"})
	start.Next(end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(start).Done()
	ts.dagRegistry.Add(d)
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	delay := ts.config.CheckDependenciesStatusWait
	sharedState := newDagRunSharedState(d.TaskParents())
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "start", 0}
	drtEnd := DagRunTask{dagrun.DagId, dagrun.AtTime, "end", 0}

	// Execute
	ts.walkAndSchedule(ctx, dagrun, d.Root, sharedState, &wg)
	time.Sleep(delay)
	// Manually mark "start" task as success, to go to another task
	uErr := ts.UpsertTaskStatus(ctx, drtStart, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Error while marking <start> as Success: %s", uErr.Error())
	}
	wg.Wait()

	// Assert
	testTaskCacheQueueTableSize(ts, 2, t)
	testTaskStatusInCache(ts, drtEnd, dag.TaskScheduled, t)
}

// Testing walkAndSchedule on the following DAG:
//
//	   n21
//	 /     \
//	/       \
//
// n1 -- n22 -- n3
//
//	\       /
//	 \     /
//	   n23
func TestWalkAndScheduleOnAsyncTasks(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	ts.dagRegistry.Add(d)
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	delay := ts.config.CheckDependenciesStatusWait
	sharedState := newDagRunSharedState(d.TaskParents())
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "n1", 0}
	drtN21 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n21", 0}
	drtN22 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n22", 0}
	drtN23 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n23", 0}
	drtN3 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n3", 0}

	ts.walkAndSchedule(ctx, dagrun, d.Root, sharedState, &wg)
	time.Sleep(delay)

	uErr := ts.UpsertTaskStatus(ctx, drtStart, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Error while updating dag run task status: %s", uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// At this point n1 should have status Success, and n21, n22 and n23 should
	// be Scheduled.
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, dag.TaskScheduled, t)
	if _, n3Exists := ts.taskCache.Get(drtN3.Base()); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	uErr = ts.UpsertTaskStatus(ctx, drtN22, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN22,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 still should not be scheduled yet
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, dag.TaskScheduled, t)
	testTaskStatusInDB(ts, drtN21, dag.TaskScheduled, t)
	testTaskStatusInCache(ts, drtN22, dag.TaskSuccess, t)
	testTaskStatusInDB(ts, drtN22, dag.TaskSuccess, t)
	if _, n3Exists := ts.taskCache.Get(drtN3.Base()); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	uErr = ts.UpsertTaskStatus(ctx, drtN21, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN21,
			uErr.Error())
	}
	uErr = ts.UpsertTaskStatus(ctx, drtN23, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN23,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 should be scheduled now, n21, n22 and n23 are done
	testTaskCacheQueueTableSize(ts, 5, t)
	testTaskStatusInCache(ts, drtN21, dag.TaskSuccess, t)
	testTaskStatusInDB(ts, drtN21, dag.TaskSuccess, t)
	testTaskStatusInCache(ts, drtN22, dag.TaskSuccess, t)
	testTaskStatusInDB(ts, drtN22, dag.TaskSuccess, t)
	testTaskStatusInCache(ts, drtN23, dag.TaskSuccess, t)
	testTaskStatusInDB(ts, drtN23, dag.TaskSuccess, t)
	testTaskStatusInCache(ts, drtN3, dag.TaskScheduled, t)
	testTaskStatusInDB(ts, drtN3, dag.TaskScheduled, t)
	wg.Wait()
}

func testTaskCacheQueueTableSize(ts *TaskScheduler, expectedCount int, t *testing.T) {
	if ts.taskCache.Len() != expectedCount {
		t.Errorf("Expected %d tasks in the cache, got: %d", expectedCount,
			ts.taskCache.Len())
	}
	if ts.taskQueue.Size() != expectedCount {
		t.Errorf("Expected %d tasks on the TaskQueue, got: %d",
			expectedCount, ts.taskQueue.Size())
	}
	rowCnt := ts.dbClient.Count("dagruntasks")
	if rowCnt != expectedCount {
		t.Errorf("Expected %d row in dagruntasks table, got: %d", expectedCount,
			rowCnt)
	}
}

func TestScheduleDagTasksSimple131(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, 100, t,
	)
}

func TestScheduleDagTasksSimple131ShortQueue(t *testing.T) {
	const taskQueueSize = 3
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_short_queue").AddSchedule(schedule).AddRoot(nodes131()).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, taskQueueSize, t,
	)
}

// Task queue is shorter then number of children to schedule during the single
// iteration.
func TestScheduleDagTasksSimple131ShortestQueue(t *testing.T) {
	const taskQueueSize = 1
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_shortest_queue").AddSchedule(schedule).AddRoot(nodes131()).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, taskQueueSize, t,
	)
}

func TestScheduleDagTasks131WithFailedTask(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_fail_n23").
		AddSchedule(schedule).
		AddRoot(nodes131()).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	taskIdsToFail := map[string]struct{}{
		"n23": {},
	}
	testScheduleDagTasksSingleDagrunWithFailure(
		d, dagrun, taskIdsToFail, 50*time.Millisecond, 100, t,
	)
}

func TestScheduleDagTasks131WithFailedFirstTask(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_fail_n1").
		AddSchedule(schedule).
		AddRoot(nodes131()).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	taskIdsToFail := map[string]struct{}{
		"n1": {},
	}
	testScheduleDagTasksSingleDagrunWithFailure(
		d, dagrun, taskIdsToFail, 50*time.Millisecond, 100, t,
	)
}

func TestScheduleDagTasks131WithFailedLastTask(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_fail_n3").
		AddSchedule(schedule).
		AddRoot(nodes131()).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	taskIdsToFail := map[string]struct{}{
		"n3": {},
	}
	testScheduleDagTasksSingleDagrunWithFailure(
		d, dagrun, taskIdsToFail, 50*time.Millisecond, 100, t,
	)
}

func TestScheduleDagTasksLinkedListShort(t *testing.T) {
	testScheduleDagTasksLinkedList(10, t)
}

func TestScheduleDagTasksLinkedListLong(t *testing.T) {
	testScheduleDagTasksLinkedList(100, t)
}

func TestScheduleDagTasksLinkedListShortQueue(t *testing.T) {
	const taskQueueSize = 2
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.
		New(dag.Id("mock_dag_ll_short_queue")).
		AddSchedule(schedule).
		AddRoot(nodesLinkedList(25)).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, taskQueueSize, t,
	)
}

func TestScheduleDagTasksLinkedListShortFailure(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	root := nodesLinkedList(2)
	d := dag.New("mock_dag_ll_short_fail").
		AddSchedule(schedule).
		AddRoot(root).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	taskIdsToFail := map[string]struct{}{
		"Start": {},
	}
	testScheduleDagTasksSingleDagrunWithFailure(
		d, dagrun, taskIdsToFail, 50*time.Millisecond, 100, t,
	)
}

func TestScheduleDagTasksNoTasks(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_no_tasks").
		AddSchedule(schedule).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, 10, t,
	)
}

func TestScheduleDagTasksSingleTask(t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	theOnlyNode := dag.NewNode(EmptyTask{TaskId: "start"})
	d := dag.New("mock_dag_single_task").
		AddSchedule(schedule).
		AddRoot(theOnlyNode).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, 10, t,
	)
}

func testScheduleDagTasksLinkedList(size int, t *testing.T) {
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.
		New(dag.Id(fmt.Sprintf("mock_dag_ll_%d", size))).
		AddSchedule(schedule).
		AddRoot(nodesLinkedList(size)).
		Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	testScheduleDagTasksSingleDagrun(
		d, dagrun, 50*time.Millisecond, size, t,
	)
}

func TestScheduleDagTasksLinkedListAfterRestart(t *testing.T) {
	const qLen = 100
	const dagId = "sample_ll"
	const llSize = 10
	d := linkedListDagSchedule1Min(dagId, llSize)
	ts := defaultTaskScheduler(t, qLen)
	ts.dagRegistry.Add(d)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	errsChan := make(chan taskSchedulerError)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	t0 := (*d.Schedule).Start()
	t0Str := timeutils.ToString(t0)

	// Insert DAG run
	_, iErr := ts.dbClient.InsertDagRun(ctx, dagId, timeutils.ToString(t0))
	if iErr != nil {
		t.Errorf("Cannot insert dag run for %s, %+v: %s", dagId, t0, iErr.Error())
	}

	// Insert few tasks to be done, to simulate starting scheduling after the
	// restart
	tasks := d.Flatten()
	success := dag.TaskSuccess.String()
	for i := 0; i < llSize/2; i++ {
		taskId := tasks[i].Id()
		iErr = ts.dbClient.InsertDagRunTask(ctx, dagId, t0Str, taskId, 0, success)
		if iErr != nil {
			t.Errorf("Cannot insert DAG run task %s.%s at %s: %s",
				dagId, taskId, t0Str, iErr.Error())
		}
	}
	// Read InsertTs from the calculation before the simulated restart
	startTaskId := (*d.Root).Task.Id()
	startInsertTsBefore, startStatusUpdateTsBefore := readDagRunTaskInsertAndUpdateTs(
		ts.dbClient, ctx, dagId, t0Str, startTaskId, t,
	)

	// Asserts before scheduling tasks
	statusSuccess := fmt.Sprintf("Status='%s'", success)
	drc := ts.dbClient.Count("dagruns")
	if drc != 1 {
		t.Errorf("Expected 1 DAG run in the database, got: %d", drc)
	}
	drcSuccess := ts.dbClient.CountWhere("dagruns", statusSuccess)
	if drcSuccess != 0 {
		t.Errorf("Expected 0 successful DAG run in the database before running scheduler, got: %d",
			drcSuccess)
	}
	drtc := ts.dbClient.Count("dagruntasks")
	if drtc != llSize/2 {
		t.Errorf("Expected %d DAG run tasks in the database after running scheduler, but got: %d",
			llSize/2, drtc)
	}

	go listenOnSchedulerErrors(errsChan, t)
	go markSuccessAllTasks(ctx, ts, 1*time.Millisecond, t)
	dr := DagRun{DagId: d.Id, AtTime: t0}
	ts.scheduleDagTasks(ctx, d, dr, errsChan)
	t.Log("Dag run is done!")

	// Asserts after scheduleDagTasks is done.
	drcAfter := ts.dbClient.CountWhere("dagruns", statusSuccess)
	if drcAfter != 1 {
		t.Errorf("Expected 1 successful DAG run in the database, got: %d",
			drcAfter)
	}
	drtcAfter := ts.dbClient.CountWhere("dagruntasks", statusSuccess)
	if drtcAfter != llSize {
		t.Errorf("Expected %d successful DAG run tasks in the database after running scheduler, but got: %d",
			llSize, drtcAfter)
	}

	startInsertTsAfter, startStatusUpdateTsAfter := readDagRunTaskInsertAndUpdateTs(
		ts.dbClient, ctx, dagId, t0Str, startTaskId, t,
	)
	if !startInsertTsBefore.Equal(startInsertTsAfter) {
		t.Errorf("Expected the same InsertTs value for task <Start> before and "+
			"after the restart. Expected %+v, but got: %+v",
			startInsertTsBefore, startInsertTsAfter)
	}
	if !startStatusUpdateTsBefore.Equal(startStatusUpdateTsAfter) {
		t.Errorf("Expected the same StatusUpdateTs value for task <Start> "+
			"before and after the restart. Expected %+v, but got %+v",
			startStatusUpdateTsBefore, startStatusUpdateTsAfter)
	}
}

func linkedListDagSchedule1Min(id string, size int) dag.Dag {
	root := nodesLinkedList(size)
	start := time.Date(2024, time.January, 16, 12, 0, 0, 0, time.UTC)
	sched := schedule.NewFixed(start, 1*time.Minute)

	d := dag.New(dag.Id(id)).
		AddSchedule(sched).
		AddRoot(root).
		Done()
	return d
}

func testScheduleDagTasksSingleDagrun(
	d dag.Dag,
	dagrun DagRun,
	delay time.Duration,
	queueLength int,
	t *testing.T,
) {
	ts := defaultTaskScheduler(t, queueLength)
	ts.dagRegistry.Add(d)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	taskNum := len(d.Flatten())
	errsChan := make(chan taskSchedulerError)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	_, iErr := ts.dbClient.InsertDagRun(
		ctx, string(d.Id), timeutils.ToString(dagrun.AtTime),
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run %v: %s", dagrun, iErr.Error())
	}

	go listenOnSchedulerErrors(errsChan, t)
	go markSuccessAllTasks(ctx, ts, delay, t)
	ts.scheduleDagTasks(ctx, d, dagrun, errsChan)
	t.Log("Dag run is done!")

	// Asssertions after the dag run is done
	statusValue := fmt.Sprintf("Status='%s'", dag.TaskSuccess)
	cnt := ts.dbClient.CountWhere("dagruntasks", statusValue)
	if cnt != taskNum {
		t.Errorf("Expected %d tasks with %s status, got: %d",
			taskNum, dag.TaskSuccess, cnt)
	}
	cnt = ts.dbClient.CountWhere("dagruns", statusValue)
	if cnt != 1 {
		t.Errorf("Expected 1 successful dagrun %v, got: %d",
			dagrun, cnt)
	}
}

func testScheduleDagTasksSingleDagrunWithFailure(
	d dag.Dag,
	dagrun DagRun,
	taskIdsToFail map[string]struct{},
	delay time.Duration,
	queueLength int,
	t *testing.T,
) {
	ts := defaultTaskScheduler(t, queueLength)
	ts.dagRegistry.Add(d)

	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	errsChan := make(chan taskSchedulerError)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	_, iErr := ts.dbClient.InsertDagRun(
		ctx, string(d.Id), timeutils.ToString(dagrun.AtTime),
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run %v: %s", dagrun, iErr.Error())
	}

	go listenOnSchedulerErrors(errsChan, t)
	go markSuccessAllTasksExceptFew(ctx, ts, taskIdsToFail, delay, t)
	ts.scheduleDagTasks(ctx, d, dagrun, errsChan)
	t.Log("Dag run is done!")

	// Asssertions after the dag run is done
	statusValue := fmt.Sprintf("Status='%s'", dag.TaskFailed)
	cnt := ts.dbClient.CountWhere("dagruntasks", statusValue)
	if cnt != len(taskIdsToFail) {
		t.Errorf("Expected %d tasks with %s status, got: %d",
			len(taskIdsToFail), dag.TaskFailed, cnt)
	}
	cnt = ts.dbClient.CountWhere("dagruns", statusValue)
	if cnt != 1 {
		t.Errorf("Expected 1 failed dagrun %v, got: %d",
			dagrun, cnt)
	}
}

func TestScheduleDagTasksSimple131TwoDagRuns(t *testing.T) {
	// Setup
	const delay = 50 * time.Millisecond
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag_131_two").
		AddSchedule(schedule).
		AddRoot(nodes131()).
		Done()
	ts.dagRegistry.Add(d)
	taskNum := len(d.Flatten())
	dagrun1 := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	dagrun2 := DagRun{DagId: d.Id, AtTime: schedule.Next(dagrun1.AtTime, &dagrun1.AtTime)}
	errsChan := make(chan taskSchedulerError)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	_, iErr := ts.dbClient.InsertDagRun(
		ctx, string(d.Id), timeutils.ToString(dagrun1.AtTime),
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run %v: %s", dagrun1, iErr.Error())
	}
	_, iErr = ts.dbClient.InsertDagRun(
		ctx, string(d.Id), timeutils.ToString(dagrun2.AtTime),
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run %v: %s", dagrun2, iErr.Error())
	}

	// Schedule two dag runs in parallel
	var wg sync.WaitGroup
	wg.Add(2)
	go listenOnSchedulerErrors(errsChan, t)
	go markSuccessAllTasks(ctx, ts, delay, t)
	go func() {
		ts.scheduleDagTasks(ctx, d, dagrun1, errsChan)
		wg.Done()
	}()
	go func() {
		ts.scheduleDagTasks(ctx, d, dagrun2, errsChan)
		wg.Done()
	}()
	wg.Wait()
	t.Log("Dag run is done!")

	// Asssertions after the dag run is done
	statusValue := fmt.Sprintf("Status='%s'", dag.TaskSuccess)
	cnt := ts.dbClient.CountWhere("dagruntasks", statusValue)
	if cnt != 2*taskNum {
		t.Errorf("Expected %d tasks with %s status, got: %d",
			2*taskNum, dag.TaskSuccess, cnt)
	}
	cnt = ts.dbClient.CountWhere("dagruns", statusValue)
	if cnt != 2 {
		t.Errorf("Expected 2 successful dagruns, got: %d", cnt)
	}
}

func TestAllTasksAreDoneSimple(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	ts.dagRegistry.Add(d)
	tasks := d.Flatten()
	sharedState := newDagRunSharedState(d.TaskParents())
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	ctx := context.Background()

	areDoneBefore := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneBefore {
		t.Errorf("All dag run %v tasks should not yet be finished", dagrun)
	}

	drtn1 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n1", 0}
	uErr := ts.UpsertTaskStatus(ctx, drtn1, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Cannot upsert task status for %v and %s", drtn1,
			dag.TaskSuccess.String())
	}

	areDoneAfterN1 := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneAfterN1 {
		t.Errorf("All dag run %v tasks should not yet be finished (after n1)",
			dagrun)
	}

	for _, taskId := range []string{"n21", "n22", "n23"} {
		drt := DagRunTask{dagrun.DagId, dagrun.AtTime, taskId, 0}
		uErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskSuccess, nil)
		if uErr != nil {
			t.Errorf("Cannot upsert task status for %v and %s", drt,
				dag.TaskSuccess.String())
		}
	}

	areDoneAfterN2x := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneAfterN2x {
		t.Errorf("All dag run %v tasks should not yet be finished (after n2x)",
			dagrun)
	}

	drtn3 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n3", 0}
	errStr := "some error"
	uErr = ts.UpsertTaskStatus(ctx, drtn3, dag.TaskFailed, &errStr)
	if uErr != nil {
		t.Errorf("Cannot upsert task status for %v and %s", drtn1,
			dag.TaskSuccess.String())
	}
	areDoneAfterN3 := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if !areDoneAfterN3 {
		t.Errorf("All dag run %v tasks should be finished after n3, but are not",
			dagrun)
	}
}

func TestAllTasksAreDoneDbFallback(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := schedule.NewFixed(startTs, 30*time.Second)
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	ts.dagRegistry.Add(d)
	tasks := d.Flatten()
	sharedState := newDagRunSharedState(d.TaskParents())
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs, nil)}
	ctx := context.Background()

	areDoneBefore := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneBefore {
		t.Errorf("All dag run %v tasks should not yet be finished", dagrun)
	}

	drtn1 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n1", 0}
	uErr := ts.UpsertTaskStatus(ctx, drtn1, dag.TaskSuccess, nil)
	if uErr != nil {
		t.Errorf("Cannot upsert task status for %v and %s", drtn1,
			dag.TaskSuccess.String())
	}

	areDoneAfterN1 := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneAfterN1 {
		t.Errorf("All dag run %v tasks should not yet be finished (after n1)",
			dagrun)
	}

	for _, taskId := range []string{"n21", "n22", "n23"} {
		drt := DagRunTask{dagrun.DagId, dagrun.AtTime, taskId, 0}
		uErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskSuccess, nil)
		if uErr != nil {
			t.Errorf("Cannot upsert task status for %v and %s", drt,
				dag.TaskSuccess.String())
		}
	}

	areDoneAfterN2x := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if areDoneAfterN2x {
		t.Errorf("All dag run %v tasks should not yet be finished (after n2x)",
			dagrun)
	}

	// Insert drt n3 status into database but not cache
	drtn3 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n3", 0}
	iErr := ts.dbClient.InsertDagRunTask(
		ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime), "n3",
		0, db.DagRunTaskStatusScheduled,
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run task %v: %s", drtn3, iErr.Error())
	}
	uErr = ts.dbClient.UpdateDagRunTaskStatus(
		ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime), "n3",
		0, dag.TaskSuccess.String(),
	)
	if uErr != nil {
		t.Errorf("Cannot update dag run task %v status %s: %s",
			drtn3, dag.TaskSuccess.String(), uErr.Error())
	}

	areDoneAfterN3 := ts.allTasksAreDone(dagrun, tasks, sharedState)
	if !areDoneAfterN3 {
		t.Errorf("All dag run %v tasks should be finished after n3, but are not",
			dagrun)
	}
}

func TestCheckIfAlreadyFinishedNoTask(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	dagrun := DagRun{dag.Id("mock_dag"), time.Now()}
	taskId := "mock_task_id"
	expectedStatus := dag.TaskNoStatus

	isFinished, status := ts.checkIfAlreadyFinished(dagrun, taskId)
	testAlreadyFinishedTaskExpectedStatus(
		isFinished, false, status, expectedStatus, t,
	)
}

func TestCheckIfAlreadyFinishedInCache(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	dagId := "mock_dag"
	dagrun := DagRun{dag.Id(dagId), time.Now()}

	// test cases
	data := []struct {
		taskId             string
		expectedStatus     dag.TaskStatus
		expectedIsFinished bool
	}{
		{"task1", dag.TaskScheduled, false},
		{"task2", dag.TaskRunning, false},
		{"task3", dag.TaskNoStatus, false},
		{"task4", dag.TaskFailed, true},
		{"task5", dag.TaskSuccess, true},
	}

	for _, d := range data {
		drt := DagRunTask{dagrun.DagId, dagrun.AtTime, d.taskId, 0}
		status := DagRunTaskState{d.expectedStatus, dagrun.AtTime.Add(1 * time.Second)}
		ts.taskCache.Put(drt.Base(), status)

		isFinished, tStatus := ts.checkIfAlreadyFinished(dagrun, d.taskId)
		testAlreadyFinishedTaskExpectedStatus(
			isFinished, d.expectedIsFinished, tStatus, d.expectedStatus, t,
		)
	}
}

func TestCheckIfAlreadyFinishedInDb(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)
	ctx := context.Background()
	dagId := "mock_dag"
	dagrun := DagRun{dag.Id(dagId), time.Now()}
	execTs := timeutils.ToString(dagrun.AtTime)

	// test cases
	data := []struct {
		taskId             string
		expectedStatus     dag.TaskStatus
		expectedIsFinished bool
	}{
		{"task1", dag.TaskScheduled, false},
		{"task2", dag.TaskRunning, false},
		{"task3", dag.TaskNoStatus, false},
		{"task4", dag.TaskFailed, true},
		{"task5", dag.TaskSuccess, true},
	}

	for _, d := range data {
		iErr := ts.dbClient.InsertDagRunTask(
			ctx, dagId, execTs, d.taskId, 0, d.expectedStatus.String(),
		)
		if iErr != nil {
			t.Errorf("Cannot insert new DAG run task into DB: %s",
				iErr.Error())
		}

		isFinished, tStatus := ts.checkIfAlreadyFinished(dagrun, d.taskId)
		testAlreadyFinishedTaskExpectedStatus(
			isFinished, d.expectedIsFinished, tStatus, d.expectedStatus, t,
		)
	}
}

func testAlreadyFinishedTaskExpectedStatus(
	isFinished, expIsFinished bool, status, expectedStatus dag.TaskStatus, t *testing.T,
) {
	t.Helper()

	if isFinished != expIsFinished {
		t.Errorf("Expected task status to be finished=%v (%+v), but got: %v (%+v)",
			expIsFinished, expectedStatus, isFinished, status)
	}
	if status != expectedStatus {
		t.Errorf("Expected task status %+v, got: %+v", expectedStatus, status)
	}
}

func TestGetDagRunTaskStatusFromCache(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)

	// Add entry to the cache
	taskId := "task_1"
	dagrun := DagRun{dag.Id("mock_dag"), time.Now()}
	drt := DagRunTask{dagrun.DagId, dagrun.AtTime, taskId, 0}
	status := DagRunTaskState{dag.TaskSuccess, dagrun.AtTime}
	ts.taskCache.Put(drt.Base(), status)

	// Get dag run task status
	statusNew, getErr := ts.getDagRunTaskStatus(dagrun, taskId)
	if getErr != nil {
		t.Errorf("Error while getting (%v, %s) dag run task status: %s",
			dagrun, taskId, getErr.Error())
	}
	if statusNew != status.Status {
		t.Errorf("Expected %v, got: %v", status.Status.String(),
			statusNew.String())
	}
}

func TestGetDagRunTaskStatusFromDatabase(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)

	// Add entry to the database and left task cache empty on purpose
	taskId := "task_1"
	execTs := time.Now()
	execTsStr := timeutils.ToString(execTs)
	dagrun := DagRun{dag.Id("mock_dag"), execTs}
	drt := DagRunTask{dagrun.DagId, dagrun.AtTime, taskId, 0}
	status := DagRunTaskState{dag.TaskSuccess, dagrun.AtTime}

	ctx := context.Background()
	iErr := ts.dbClient.InsertDagRunTask(
		ctx, string(dagrun.DagId), execTsStr, taskId, 0,
		dag.TaskSuccess.String(),
	)
	if iErr != nil {
		t.Errorf("Cannot insert dag run task (%v) to database: %s",
			drt, iErr.Error())
	}

	// Get dag run task status
	statusNew, getErr := ts.getDagRunTaskStatus(dagrun, taskId)
	if getErr != nil {
		t.Errorf("Error while getting (%v, %s) dag run task status: %s",
			dagrun, taskId, getErr.Error())
	}
	if statusNew != status.Status {
		t.Errorf("Expected %v, got: %v", status.Status.String(),
			statusNew.String())
	}

	// Check if this dag run task is also in the cache
	drts, exists := ts.taskCache.Get(drt.Base())
	if !exists {
		t.Errorf("Expected %v to exist in the task cache, but it's not",
			drt)
	}
	if drts.Status != status.Status {
		t.Errorf("Expected status from cache %s, but got %s",
			status.Status.String(), drts.Status.String())
	}
}

func TestGetDagRunTaskStatusNoCacheNoDatabase(t *testing.T) {
	ts := defaultTaskScheduler(t, 10)
	defer db.CleanUpSqliteTmp(ts.dbClient, t)

	// In this case we don't add entry either to the cache nor to database
	taskId := "task_1"
	dagrun := DagRun{dag.Id("mock_dag"), time.Now()}

	// Get dag run task status
	statusNew, getErr := ts.getDagRunTaskStatus(dagrun, taskId)
	if getErr != sql.ErrNoRows {
		t.Errorf("Expected no rows error, got: %s", getErr.Error())
	}
	if statusNew != dag.TaskNoStatus {
		t.Errorf("Expected %v, got: %v", dag.TaskNoStatus.String(),
			statusNew.String())
	}
}

// Marks all tasks popped from the queue as success.
func markSuccessAllTasks(
	ctx context.Context,
	ts *TaskScheduler,
	taskExecutionDuration time.Duration,
	t *testing.T,
) {
	delay := ts.config.CheckDependenciesStatusWait
	for {
		select {
		case <-ctx.Done():
			t.Log("Breaking markSuccessAllTasks, because context is done")
			return
		default:
		}
		drt, popErr := ts.taskQueue.Pop()
		if popErr == ds.ErrQueueIsEmpty {
			time.Sleep(delay)
			continue
		}
		if popErr != nil {
			t.Errorf("Error while popping dag run task from the queue: %s",
				popErr.Error())
		}
		time.Sleep(taskExecutionDuration) // executor work simulation
		uErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskSuccess, nil)
		if uErr != nil {
			t.Errorf("Error while marking %v as success: %s",
				drt, uErr.Error())
		}
	}
}

// Mark all tasks popped from the queue as success excepts keys from
// taskIdsToFail which are marked as Failed.
func markSuccessAllTasksExceptFew(
	ctx context.Context,
	ts *TaskScheduler,
	taskIdsToFail map[string]struct{},
	taskExecutionDuration time.Duration,
	t *testing.T,
) {
	delay := ts.config.CheckDependenciesStatusWait
	for {
		select {
		case <-ctx.Done():
			t.Log("Breaking markSuccessAllTasks, because context is done")
			return
		default:
		}
		drt, popErr := ts.taskQueue.Pop()
		if popErr == ds.ErrQueueIsEmpty {
			time.Sleep(delay)
			continue
		}
		if popErr != nil {
			t.Errorf("Error while popping dag run task from the queue: %s",
				popErr.Error())
		}
		time.Sleep(taskExecutionDuration) // executor work simulation
		if _, shouldFail := taskIdsToFail[drt.TaskId]; shouldFail {
			errStr := "some error"
			uErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskFailed, &errStr)
			if uErr != nil {
				t.Errorf("Error while marking %v as Failed: %s",
					drt, uErr.Error())
			}
			continue
		}
		uErr := ts.UpsertTaskStatus(ctx, drt, dag.TaskSuccess, nil)
		if uErr != nil {
			t.Errorf("Error while marking %v as success: %s",
				drt, uErr.Error())
		}
	}
}

func testTaskStatusInCache(
	ts *TaskScheduler,
	drt DagRunTask,
	expectedStatus dag.TaskStatus,
	t *testing.T,
) {
	drts, exists := ts.taskCache.Get(drt.Base())
	if !exists {
		t.Errorf("Cannot get %+v from the TaskCache", drt)
	}
	if drts.Status != expectedStatus {
		t.Errorf("Expected status %s for %+v, got: %s", expectedStatus.String(),
			drt, drts.Status.String())
	}
}

func testTaskStatusInDB(
	ts *TaskScheduler,
	drt DagRunTask,
	expectedStatus dag.TaskStatus,
	t *testing.T,
) {
	ctx := context.Background()
	drtDb, dbErr := ts.dbClient.ReadDagRunTaskLatest(
		ctx, string(drt.DagId), timeutils.ToString(drt.AtTime), drt.TaskId,
	)
	if dbErr == sql.ErrNoRows {
		t.Errorf("There is no row in dagruntasks for %v", drt)
	}
	if drtDb.Status != expectedStatus.String() {
		t.Errorf("DagRunTask %v in the database has status: %s, but expected %s",
			drt, drtDb.Status, expectedStatus.String())
	}
}

func listenOnSchedulerErrors(errChan chan taskSchedulerError, t *testing.T) {
	for err := range errChan {
		t.Errorf("Got error on taskScheduler error channel for (%s, %v): %s",
			err.DagId, err.ExecTs, err.Err.Error())
	}
}

// Initialize default TaskScheduler with in-memory DB client for testing.
func defaultTaskScheduler(t *testing.T, taskQueueCap int) *TaskScheduler {
	c, err := db.NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	registry := dag.Registry{}
	drQueue := ds.NewSimpleQueue[DagRun](100)
	taskQueue := ds.NewSimpleQueue[DagRunTask](taskQueueCap)
	queues := Queues{
		DagRuns:     &drQueue,
		DagRunTasks: &taskQueue,
	}
	taskCache := ds.NewLruCache[DRTBase, DagRunTaskState](10)
	getStateFunc := func() State {
		return StateRunning
	}
	notifications := make([]string, 0)
	notifier := notify.NewMock(&notifications)

	var goroutinesCount int64
	ts := NewTaskScheduler(
		registry, c, queues, taskCache, DefaultTaskSchedulerConfig,
		testLogger(), notifier, &goroutinesCount, getStateFunc,
	)
	return ts
}

func readDagRunTaskInsertAndUpdateTs(
	c *db.Client, ctx context.Context, dagId, execTs, taskId string,
	t *testing.T,
) (time.Time, time.Time) {
	t.Helper()
	task, dbErr := c.ReadDagRunTaskLatest(
		ctx, dagId, execTs, taskId,
	)
	if dbErr != nil {
		t.Errorf("Cannot read dag run task for %s: %s", taskId, dbErr.Error())
	}
	insertTs, tsErr := timeutils.FromString(task.InsertTs)
	if tsErr != nil {
		t.Errorf("Cannot parse InsertTs timestamp based on %s: %s",
			task.InsertTs, tsErr.Error())
	}
	updateTs, tsErr := timeutils.FromString(task.StatusUpdateTs)
	if tsErr != nil {
		t.Errorf("Cannot parse StatusUpdateTs timestamp based on %s: %s",
			task.StatusUpdateTs, tsErr.Error())
	}
	return insertTs, updateTs
}

//	   n21
//	 /     \
//	/       \
//
// n1 -- n22 -- n3
//
//	\       /
//	 \     /
//	   n23
func nodes131() *dag.Node {
	n1 := dag.NewNode(EmptyTask{TaskId: "n1"})
	n21 := dag.NewNode(EmptyTask{TaskId: "n21"})
	n22 := dag.NewNode(EmptyTask{TaskId: "n22"})
	n23 := dag.NewNode(EmptyTask{TaskId: "n23"})
	n3 := dag.NewNode(EmptyTask{TaskId: "n3"})

	n1.NextAsyncAndMerge([]*dag.Node{n21, n22, n23}, n3)
	return n1
}

func nodesLinkedList(length int) *dag.Node {
	s := dag.NewNode(EmptyTask{TaskId: "Start"})
	prev := s
	for i := 0; i < length-1; i++ {
		n := dag.NewNode(EmptyTask{TaskId: fmt.Sprintf("step_%d", i)})
		prev = prev.Next(n)
	}
	return s
}
