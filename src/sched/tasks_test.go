package sched

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/timeutils"
)

type EmptyTask struct {
	TaskId string
}

func (et EmptyTask) Id() string { return et.TaskId }
func (et EmptyTask) Execute()   { fmt.Println(et.TaskId) }

func TestCheckIfCanBeScheduledFirstTask(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.DbClient, t)
	start := dag.Node{Task: EmptyTask{"start"}}
	end := dag.Node{Task: EmptyTask{"end"}}
	start.Next(&end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := dag.FixedSchedule{Start: startTs, Interval: 30 * time.Second}
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(&start).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs)}

	startShouldBeSched := ts.checkIfCanBeScheduled(
		dagrun, "start", d.TaskParents(),
	)
	if !startShouldBeSched {
		t.Error("Task <start> should be scheduled, but it's not")
	}

	endShouldBeSched := ts.checkIfCanBeScheduled(
		dagrun, "end", d.TaskParents(),
	)
	if endShouldBeSched {
		t.Error("Task <end> should not be scheduled in this case, but it is")
	}
}

func TestScheduleSingleTaskSimple(t *testing.T) {
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.DbClient, t)
	start := dag.Node{Task: EmptyTask{"start"}}
	end := dag.Node{Task: EmptyTask{"end"}}
	start.Next(&end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := dag.FixedSchedule{Start: startTs, Interval: 30 * time.Second}
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(&start).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs)}
	taskId := "start"

	ts.scheduleSingleTask(dagrun, taskId)

	// Task should be on the TaskQeueu
	expectedDrt := DagRunTask{
		DagId:  dagrun.DagId,
		AtTime: dagrun.AtTime,
		TaskId: taskId,
	}
	drt, popErr := ts.TaskQueue.Pop()
	if popErr != nil {
		t.Errorf("Error while popping task from the queue: %s", popErr.Error())
	}
	if drt != expectedDrt {
		t.Errorf("Expected popped dag run task from queue to be %v, got %v",
			expectedDrt, drt)
	}

	// Task's status should be cached
	cacheStatus, existInCache := ts.TaskCache.Get(drt)
	if !existInCache {
		t.Errorf("Scheduled task %v does not exist in TaskCache", drt)
	}
	if cacheStatus.Status != Scheduled {
		t.Errorf("Expected cached status of %v, to be %s, got %s",
			drt, Scheduled.String(), cacheStatus.Status.String())
	}

	// Task should be inserted into the database
	ctx := context.Background()
	drtDb, dbErr := ts.DbClient.ReadDagRunTask(
		ctx, string(dagrun.DagId), timeutils.ToString(dagrun.AtTime), taskId,
	)
	if dbErr == sql.ErrNoRows {
		t.Errorf("There is no row in dagruntasks for %v", drt)
	}
	if drtDb.Status != Scheduled.String() {
		t.Errorf("DagRunTask %v in the database has status: %s, but expected %s",
			drt, drtDb.Status, Scheduled.String())
	}
}

func TestWalkAndScheduleOnTwoTasks(t *testing.T) {
	// Prepare scenario
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.DbClient, t)
	start := dag.Node{Task: EmptyTask{"start"}}
	end := dag.Node{Task: EmptyTask{"end"}}
	start.Next(&end)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := dag.FixedSchedule{Start: startTs, Interval: 30 * time.Second}
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(&start).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs)}
	var wg sync.WaitGroup
	wg.Add(2)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	alreadyMarkedTasks := ds.NewAsyncMap[DagRunTask, any]()
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "start"}
	drtEnd := DagRunTask{dagrun.DagId, dagrun.AtTime, "end"}

	// Execute
	ts.walkAndSchedule(ctx, dagrun, d.Root, d.TaskParents(), alreadyMarkedTasks, &wg)
	time.Sleep(50 * time.Millisecond)
	// Manually mark "start" task as success, to go to another task
	uErr := ts.UpsertTaskStatus(ctx, drtStart, Success)
	if uErr != nil {
		t.Errorf("Error while marking <start> as Success: %s", uErr.Error())
	}
	wg.Wait()

	// Assert
	testTaskCacheQueueTableSize(ts, 2, t)
	testTaskStatusInCache(ts, drtEnd, Scheduled, t)
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
	t.Logf("Preparing scenario...")
	ts := defaultTaskScheduler(t, 100) // cache and DB is empty at this point
	defer db.CleanUpSqliteTmp(ts.DbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := dag.FixedSchedule{Start: startTs, Interval: 30 * time.Second}
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs)}
	var wg sync.WaitGroup
	wg.Add(5)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	alreadyMarkedTasks := ds.NewAsyncMap[DagRunTask, any]()
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "n1"}
	drtN21 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n21"}
	drtN22 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n22"}
	drtN23 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n23"}
	drtN3 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n3"}

	t.Logf("Start ts.walkAndSchedule...")
	ts.walkAndSchedule(ctx, dagrun, d.Root, d.TaskParents(), alreadyMarkedTasks, &wg)
	time.Sleep(50 * time.Millisecond)

	t.Logf("Manually mark n1 as success")
	uErr := ts.UpsertTaskStatus(ctx, drtStart, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task status: %s", uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// At this point n1 should have status Success, and n21, n22 and n23 should
	// be Scheduled.
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, Scheduled, t)
	if _, n3Exists := ts.TaskCache.Get(drtN3); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	t.Logf("Manually mark n22 (just one of three scheduled tasks at the same time) as success")
	uErr = ts.UpsertTaskStatus(ctx, drtN22, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN22,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 still should not be scheduled yet
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, Scheduled, t)
	testTaskStatusInDB(ts, drtN21, Scheduled, t)
	testTaskStatusInCache(ts, drtN22, Success, t)
	testTaskStatusInDB(ts, drtN22, Success, t)
	if _, n3Exists := ts.TaskCache.Get(drtN3); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	t.Logf("Mark n21 and n23 (the rest of async tasks) as Success")
	uErr = ts.UpsertTaskStatus(ctx, drtN21, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN21,
			uErr.Error())
	}
	uErr = ts.UpsertTaskStatus(ctx, drtN23, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN23,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 should be scheduled now, n21, n22 and n23 are done
	testTaskCacheQueueTableSize(ts, 5, t)
	testTaskStatusInCache(ts, drtN21, Success, t)
	testTaskStatusInDB(ts, drtN21, Success, t)
	testTaskStatusInCache(ts, drtN22, Success, t)
	testTaskStatusInDB(ts, drtN22, Success, t)
	testTaskStatusInCache(ts, drtN23, Success, t)
	testTaskStatusInDB(ts, drtN23, Success, t)
	testTaskStatusInCache(ts, drtN3, Scheduled, t)
	testTaskStatusInDB(ts, drtN3, Scheduled, t)
	wg.Wait()
}

/*
// TODO: Go back to this test case when full task life cycle will be
// implemented.

// Testing walkAndSchedule on the following DAG using very small TaskQueue:
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
func TestWalkAndScheduleOnAsyncTasksSmallQueue(t *testing.T) {
	const taskQueueCapacity = 2
	t.Logf("Preparing scenario with task queue capacity: %d", taskQueueCapacity)

	ts := defaultTaskScheduler(t, taskQueueCapacity)
	defer db.CleanUpSqliteTmp(ts.DbClient, t)
	var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	schedule := dag.FixedSchedule{Start: startTs, Interval: 30 * time.Second}
	d := dag.New("mock_dag").AddSchedule(schedule).AddRoot(nodes131()).Done()
	dagrun := DagRun{DagId: d.Id, AtTime: schedule.Next(startTs)}
	var wg sync.WaitGroup
	wg.Add(5)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	alreadyMarkedTasks := ds.NewAsyncMap[DagRunTask, any]()
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "n1"}
	drtN21 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n21"}
	drtN22 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n22"}
	drtN23 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n23"}
	drtN3 := DagRunTask{dagrun.DagId, dagrun.AtTime, "n3"}

	t.Logf("Start ts.walkAndSchedule...")
	ts.walkAndSchedule(ctx, dagrun, d.Root, d.TaskParents(), alreadyMarkedTasks, &wg)
	time.Sleep(50 * time.Millisecond)

	t.Logf("Manually mark n1 as success")
	uErr := ts.UpsertTaskStatus(ctx, drtStart, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task status: %s", uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// At this point n1 should have status Success, and n21, n22 and n23 should
	// be Scheduled.
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, Scheduled, t)
	if _, n3Exists := ts.TaskCache.Get(drtN3); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	t.Logf("Manually mark n22 (just one of three scheduled tasks at the same time) as success")
	uErr = ts.UpsertTaskStatus(ctx, drtN22, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN22,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 still should not be scheduled yet
	testTaskCacheQueueTableSize(ts, 4, t)
	testTaskStatusInCache(ts, drtN21, Scheduled, t)
	testTaskStatusInDB(ts, drtN21, Scheduled, t)
	testTaskStatusInCache(ts, drtN22, Success, t)
	testTaskStatusInDB(ts, drtN22, Success, t)
	if _, n3Exists := ts.TaskCache.Get(drtN3); n3Exists {
		t.Errorf("Unexpectedly %+v exists in TaskCache before n21, n22, n23 finished",
			drtN3)
	}

	t.Logf("Mark n21 and n23 (the rest of async tasks) as Success")
	uErr = ts.UpsertTaskStatus(ctx, drtN21, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN21,
			uErr.Error())
	}
	uErr = ts.UpsertTaskStatus(ctx, drtN23, Success)
	if uErr != nil {
		t.Errorf("Error while updating dag run task %v status: %s", drtN23,
			uErr.Error())
	}
	time.Sleep(50 * time.Millisecond)

	// n3 should be scheduled now, n21, n22 and n23 are done
	testTaskCacheQueueTableSize(ts, 5, t)
	testTaskStatusInCache(ts, drtN21, Success, t)
	testTaskStatusInDB(ts, drtN21, Success, t)
	testTaskStatusInCache(ts, drtN22, Success, t)
	testTaskStatusInDB(ts, drtN22, Success, t)
	testTaskStatusInCache(ts, drtN23, Success, t)
	testTaskStatusInDB(ts, drtN23, Success, t)
	testTaskStatusInCache(ts, drtN3, Scheduled, t)
	testTaskStatusInDB(ts, drtN3, Scheduled, t)
	wg.Wait()
}
*/

func testTaskCacheQueueTableSize(ts *taskScheduler, expectedCount int, t *testing.T) {
	if ts.TaskCache.Len() != expectedCount {
		t.Errorf("Expected %d tasks in the cache, got: %d", expectedCount,
			ts.TaskCache.Len())
	}
	if ts.TaskQueue.Size() != expectedCount {
		t.Errorf("Expected %d tasks on the TaskQueue, got: %d",
			expectedCount, ts.TaskQueue.Size())
	}
	rowCnt := ts.DbClient.Count("dagruntasks")
	if rowCnt != expectedCount {
		t.Errorf("Expected %d row in dagruntasks table, got: %d", expectedCount,
			rowCnt)
	}
}

func testTaskStatusInCache(
	ts *taskScheduler,
	drt DagRunTask,
	expectedStatus DagRunTaskStatus,
	t *testing.T,
) {
	drts, exists := ts.TaskCache.Get(drt)
	if !exists {
		t.Errorf("Cannot get %+v from the TaskCache", drt)
	}
	if drts.Status != expectedStatus {
		t.Errorf("Expected status %s for %+v, got: %s", expectedStatus.String(),
			drt, drts.Status.String())
	}
}

func testTaskStatusInDB(
	ts *taskScheduler,
	drt DagRunTask,
	expectedStatus DagRunTaskStatus,
	t *testing.T,
) {
	ctx := context.Background()
	drtDb, dbErr := ts.DbClient.ReadDagRunTask(
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

// Initialize default TaskScheduler with in-memory DB client for testing.
func defaultTaskScheduler(t *testing.T, taskQueueCap int) *taskScheduler {
	c, err := db.NewSqliteTmpClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	drQueue := ds.NewSimpleQueue[DagRun](100)
	taskQueue := ds.NewSimpleQueue[DagRunTask](taskQueueCap)
	taskCache := newSimpleCache[DagRunTask, DagRunTaskState]()
	ts := taskScheduler{
		DbClient:    c,
		DagRunQueue: &drQueue,
		TaskQueue:   &taskQueue,
		TaskCache:   &taskCache,
		Config:      defaultTaskSchedulerConfig(),
	}
	return &ts
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
	n1 := dag.Node{Task: EmptyTask{TaskId: "n1"}}
	n21 := dag.Node{Task: EmptyTask{TaskId: "n21"}}
	n22 := dag.Node{Task: EmptyTask{TaskId: "n22"}}
	n23 := dag.Node{Task: EmptyTask{TaskId: "n23"}}
	n3 := dag.Node{Task: EmptyTask{TaskId: "n3"}}

	n1.NextAsyncAndMerge([]*dag.Node{&n21, &n22, &n23}, &n3)
	return &n1
}
