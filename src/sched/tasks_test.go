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
	ts := defaultTaskScheduler(t) // cache and DB is empty at this point
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
	ts := defaultTaskScheduler(t) // cache and DB is empty at this point
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
	ts := defaultTaskScheduler(t) // cache and DB is empty at this point
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
	drtStart := DagRunTask{dagrun.DagId, dagrun.AtTime, "start"}
	drtEnd := DagRunTask{dagrun.DagId, dagrun.AtTime, "end"}

	// Execute
	ts.walkAndSchedule(ctx, dagrun, d.Root, d.TaskParents(), &wg)
	time.Sleep(100 * time.Millisecond)
	// Manually mark "start" task as success, to go to another task
	ts.TaskCache.Update(drtStart, DagRunTaskState{Status: Success})
	wg.Wait()

	// Assert
	if ts.TaskCache.Len() != 2 {
		t.Errorf("Expected 2 tasks in the cache, got: %d", ts.TaskCache.Len())
	}
	if ts.TaskQueue.Size() != 2 {
		t.Errorf("Expected 2 tasks on the TaskQueue, got: %d",
			ts.TaskQueue.Size())
	}
	rowCnt := ts.DbClient.Count("dagruntasks")
	if rowCnt != 2 {
		t.Errorf("Expected 2 row in dagruntasks table, got: %d", rowCnt)
	}
	drtsEnd, endExists := ts.TaskCache.Get(drtEnd)
	if !endExists {
		t.Errorf("Cannot get %+v from the TaskCache", drtEnd)
	}
	if drtsEnd.Status != Scheduled {
		t.Errorf("Expected status %s for %+v, got: %s", Scheduled.String(),
			drtEnd, drtsEnd.Status)
	}
}

// Initialize default TaskScheduler with in-memory DB client for testing.
func defaultTaskScheduler(t *testing.T) *taskScheduler {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	drQueue := ds.NewSimpleQueue[DagRun](10)
	taskQueue := ds.NewSimpleQueue[DagRunTask](10)
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
