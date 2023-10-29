package sched

import (
	"context"
	"testing"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/timeutils"
)

func TestCacheSimple(t *testing.T) {
	cache := newSimpleCache[DagRunTask, DagRunTaskState]()

	drt := DagRunTask{
		DagId:  dag.Id("mock_dag"),
		AtTime: time.Now(),
		TaskId: "my_task_1",
	}
	drts := DagRunTaskState{
		Status:         Scheduled,
		StatusUpdateTs: drt.AtTime,
	}

	addErr := cache.Add(drt, drts)
	if addErr != nil {
		t.Errorf("Error while adding new element: %s", addErr.Error())
	}

	drts2, getErr := cache.Get(drt)
	if getErr != nil {
		t.Errorf("Error while getting existing element: %s", getErr.Error())
	}
	if drts != drts2 {
		t.Errorf("Value from the cache is different. Expected %v, got %v",
			drts, drts2)
	}

	drtsNew := DagRunTaskState{
		Status:         Scheduled,
		StatusUpdateTs: drt.AtTime.Add(1 * time.Hour),
	}
	updateErr := cache.Update(drt, drtsNew)
	if updateErr != nil {
		t.Errorf("Error while updating value for existing key: %s",
			updateErr.Error())
	}

	drts3, get2Err := cache.Get(drt)
	if get2Err != nil {
		t.Errorf("Error while getting item after the update: %s",
			get2Err.Error())
	}
	if drts3 != drtsNew {
		t.Errorf("Value from the cache is different. Expected %v, got %v",
			drtsNew, drts3)
	}

	cache.Remove(drt)
	_, get3Err := cache.Get(drt)
	if get3Err != ErrCacheKeyDoesNotExist {
		t.Errorf("Expected <key does not exist> error, got: %v", get3Err)
	}
}

func TestCachePullFromDbDagRunTask(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	cache := newSimpleCache[DagRunTask, DagRunTaskState]()

	dagId := "mock_dag_1"
	ts := time.Date(2023, 10, 5, 12, 0, 0, 0, time.UTC)
	timestamp := timeutils.ToString(ts)
	taskId := "my_task_1"

	iErr := c.InsertDagRunTask(ctx, dagId, timestamp, taskId)
	if iErr != nil {
		t.Errorf("Error while inserting new dag run task into the DB: %s",
			iErr.Error())
	}

	drt := DagRunTask{
		DagId:  dag.Id(dagId),
		AtTime: ts,
		TaskId: taskId,
	}
	/*
		drts := DagRunTaskState{
			Status:         Scheduled,
			StatusUpdateTs: drt.AtTime,
		}
	*/

	pullErr := cache.PullFromDatabase(ctx, drt, c)
	if pullErr != nil {
		t.Errorf("Error while cache pulling data from the DB: %s",
			pullErr.Error())
	}

	/* This fails as expected. I need to implement db.ReadDagRunTaskStatus...
	drtC, getErr := cache.Get(drt)
	if getErr != nil {
		t.Errorf("Error while getting item from the cache: %s", getErr.Error())
	}
	if drtC != drts {
		t.Errorf("Expected %v, got: %v", drt, drtC)
	}
	*/
}
