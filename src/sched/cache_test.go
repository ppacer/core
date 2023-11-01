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

	drts2, exists := cache.Get(drt)
	if !exists {
		t.Errorf("Expected %v to exists in the cache, but it doesn not", drt)
	}
	if drts != drts2 {
		t.Errorf("Value from the cache is different. Expected %v, got %v",
			drts, drts2)
	}
	if cache.Len() != 1 {
		t.Errorf("Expected %d items in the cache, got: %d", 1, cache.Len())
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
	if cache.Len() != 1 {
		t.Errorf("Expected %d items in the cache, got: %d", 1, cache.Len())
	}

	drts3, exists2 := cache.Get(drt)
	if !exists2 {
		t.Errorf("Expected %v to exists in the cache, but it doesn not", drt)
	}
	if drts3 != drtsNew {
		t.Errorf("Value from the cache is different. Expected %v, got %v",
			drtsNew, drts3)
	}
	if cache.Len() != 1 {
		t.Errorf("Expected %d items in the cache, got: %d", 1, cache.Len())
	}

	cache.Remove(drt)
	if _, exists3 := cache.Get(drt); exists3 {
		t.Errorf("Expected %v to be removed from the cache, but it's still there",
			drt)
	}
	if cache.Len() != 0 {
		t.Errorf("Expected %d items in the cache, got: %d", 0, cache.Len())
	}
}

func TestCachePullFromDbDagRunTask(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	cache := newSimpleCache[DagRunTask, DagRunTaskState]()

	timestampBeforeInsert := time.Now()
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

	pullErr := cache.PullFromDatabase(ctx, drt, c)
	if pullErr != nil {
		t.Errorf("Error while cache pulling data from the DB: %s",
			pullErr.Error())
	}

	drtC, exists := cache.Get(drt)
	if !exists {
		t.Errorf("Expected %v to exists in the cache, but it doesn not", drt)
	}
	if drtC.Status != Scheduled {
		t.Errorf("Expected %v, got: %v", drt, drtC)
	}
	if drtC.StatusUpdateTs.Compare(timestampBeforeInsert) <= 0 {
		t.Errorf("Expected StatusUpdateTs (%v) to be after timestampBeforeInsert (%v)",
			drtC.StatusUpdateTs, timestampBeforeInsert)
	}
}
