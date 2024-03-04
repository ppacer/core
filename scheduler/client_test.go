// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/timeutils"
)

func TestClientGetTaskEmptyDb(t *testing.T) {
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer testServer.Close()
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)

	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)
	_, err := schedClient.GetTask()
	if err == nil {
		t.Error("Expected non-nil error for GetTask on empty database")
	}
	if err != ds.ErrQueueIsEmpty {
		t.Errorf("Expected empty queue error, but got: %s", err.Error())
	}
}

func TestClientGetTaskMockSingle(t *testing.T) {
	cfg := DefaultConfig
	queues := DefaultQueues(cfg)
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(queues, cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()

	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)

	drt := DagRunTask{dag.Id("mock_dag"), time.Now(), "task1"}
	putErr := queues.DagRunTasks.Put(drt)
	if putErr != nil {
		t.Errorf("Cannot put new DAG run task onto the queue: %s",
			putErr.Error())
	}
	if queues.DagRunTasks.Size() != 1 {
		t.Errorf("Expected 1 task on the queue, got: %d",
			queues.DagRunTasks.Size())
	}

	task, err := schedClient.GetTask()
	if err != nil {
		t.Errorf("Unexpected error for GetTask(): %s", err.Error())
	}
	taskToExecEqualsDRT(task, drt, t)

	if queues.DagRunTasks.Size() != 0 {
		t.Errorf("Expected 0 task on the queue, got: %d",
			queues.DagRunTasks.Size())
	}

	_, err2 := schedClient.GetTask()
	if err2 == nil {
		t.Error("Expected non-nil error for the second GetTask()")
	}
	if err2 != ds.ErrQueueIsEmpty {
		t.Errorf("Expected empty queue error, but got: %s", err2.Error())
	}
}

func TestClientGetTaskMockMany(t *testing.T) {
	cfg := DefaultConfig
	queues := DefaultQueues(cfg)
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(queues, cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)

	dagId := dag.Id("mock_dag")
	data := []DagRunTask{
		{dagId, time.Now(), "task1"},
		{dagId, time.Now(), "task2"},
		{dagId, time.Now(), "task3"},
		{dagId, time.Now(), "task4"},
		{dagId, time.Now(), "task5"},
	}

	// Put tasks onto the task queue
	for _, drt := range data {
		putErr := queues.DagRunTasks.Put(drt)
		if putErr != nil {
			t.Errorf("Cannot put DAG run task onto the queue: %s",
				putErr.Error())
		}
	}
	size := queues.DagRunTasks.Size()
	if size != len(data) {
		t.Errorf("At this point DagrunTask queue should have %d elements, got: %d",
			len(data), size)
	}

	// Get and verify tasks
	for _, drt := range data {
		task, gtErr := schedClient.GetTask()
		if gtErr != nil {
			t.Errorf("Cannot GetTask(): %s", gtErr.Error())
		}
		taskToExecEqualsDRT(task, drt, t)
	}
}

func TestClientUpsertTaskEmptyDb(t *testing.T) {
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()

	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)
	tte := models.TaskToExec{
		DagId:  "mock_dag",
		ExecTs: timeutils.ToString(time.Now()),
		TaskId: "task1",
	}
	uErr := schedClient.UpsertTaskStatus(tte, dag.TaskSuccess)
	if uErr != nil {
		t.Errorf("Cannot insert new DAG run task on empty dagruntasks table: %s",
			uErr.Error())
	}
	tasksInDb := scheduler.dbClient.Count("dagruntasks")
	if tasksInDb != 1 {
		t.Errorf("Expected 1 row in dagruntasks table, got: %d", tasksInDb)
	}
}

func TestClientUpsertTaskSimpleUpdate(t *testing.T) {
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)

	ctx := context.Background()
	const dagId = "mock_dag"
	execTs := timeutils.ToString(time.Now())
	taskIds := []string{
		"start",
		"task1",
		"task2",
		"end",
	}
	initStatus := dag.TaskScheduled.String()
	newStatus := dag.TaskSuccess

	// Insert tasks with SCHEDULED status
	for _, taskId := range taskIds {
		iErr := scheduler.dbClient.InsertDagRunTask(ctx, dagId, execTs, taskId,
			initStatus)
		if iErr != nil {
			t.Errorf("Cannot insert DAG run task into DB: %s", iErr.Error())
		}
	}

	// Update task statuses to SUCCESS
	for _, taskId := range taskIds {
		tte := newTaskToExec(dagId, execTs, taskId)
		uErr := schedClient.UpsertTaskStatus(tte, newStatus)
		if uErr != nil {
			t.Errorf("Error while updating task status by the client: %s",
				uErr.Error())
		}
	}

	// Test statuses in the database
	where := fmt.Sprintf("Status = '%s'", newStatus.String())
	cnt := scheduler.dbClient.CountWhere("dagruntasks", where)
	if cnt != len(taskIds) {
		t.Errorf("Expected %d tasks in dagruntasks table with %s status, got: %d",
			len(taskIds), newStatus, cnt)
	}
}

func TestClientUpsertSingleTaskFewStatuses(t *testing.T) {
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)

	const dagId = "mock_dag"
	execTs := time.Now()
	execTsStr := timeutils.ToString(execTs)
	const taskId = "mock_task_id"
	tte := newTaskToExec(dagId, execTsStr, taskId)

	u1Err := schedClient.UpsertTaskStatus(tte, dag.TaskScheduled)
	if u1Err != nil {
		t.Errorf("Cannot insert new DAG run task %+v: %s", tte, u1Err.Error())
	}

	statuses := []dag.TaskStatus{
		dag.TaskRunning,
		dag.TaskNoStatus,
		dag.TaskSuccess,
	}

	prev := readDagRunTaskFromDb(scheduler.dbClient, dagId, execTsStr, taskId, t)
	for _, status := range statuses {
		uErr := schedClient.UpsertTaskStatus(tte, status)
		if uErr != nil {
			t.Errorf("Error while upserting task %+v: %s", tte, uErr.Error())
		}
		act := readDagRunTaskFromDb(scheduler.dbClient, dagId, execTsStr, taskId, t)
		if act.Status != status.String() {
			t.Errorf("Expected status %s after the update, got: %s",
				status.String(), act.Status)
		}
		prevUpdateTs := timeutils.FromStringMust(prev.StatusUpdateTs)
		actUpdateTs := timeutils.FromStringMust(act.StatusUpdateTs)
		if actUpdateTs.Before(prevUpdateTs) {
			t.Errorf("Current update timestamp %+v should be after the last one %+v",
				actUpdateTs, prevUpdateTs)
		}
	}
}

func TestClientGetStateSimple(t *testing.T) {
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()

	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)
	schedState, err := schedClient.GetState()
	if err != nil {
		t.Errorf("Error when getting scheduler state: %s", err.Error())
	}
	if schedState != StateStarted && schedState != StateSynchronizing && schedState != StateRunning {
		t.Errorf("Got unexpected Scheduler State: %s", schedState.String())
	}

	scheduler.setState(StateStopping)
	schedState2, err2 := schedClient.GetState()
	if err2 != nil {
		t.Errorf("Error when getting scheduler state for the second time: %s",
			err2.Error())
	}
	if schedState2 != StateStopping {
		t.Errorf("Expected Scheduled State %s, got: %s", StateStopping.String(),
			schedState2.String())
	}
}

func taskToExecEqualsDRT(
	task models.TaskToExec, expectedDrt DagRunTask, t *testing.T,
) {
	t.Helper()
	if task.DagId != string(expectedDrt.DagId) {
		t.Errorf("Expected DagId=%s, got %s", string(expectedDrt.DagId),
			task.DagId)
	}
	expTs := timeutils.ToString(expectedDrt.AtTime)
	if task.ExecTs != expTs {
		t.Errorf("Expected ExecTs=%s, got %s", expTs, task.ExecTs)
	}
	if task.TaskId != expectedDrt.TaskId {
		t.Errorf("Expected TaskId=%s, got %s", expectedDrt.TaskId, task.TaskId)
	}
}

func newTaskToExec(dagId, execTs, taskId string) models.TaskToExec {
	return models.TaskToExec{
		DagId:  dagId,
		ExecTs: execTs,
		TaskId: taskId,
	}
}

func readDagRunTaskFromDb(
	dbClient *db.Client, dagId, execTs, taskId string, t *testing.T,
) db.DagRunTask {
	t.Helper()
	ctx := context.Background()
	drt, dbErr := dbClient.ReadDagRunTask(ctx, dagId, execTs, taskId)
	if dbErr != nil {
		t.Errorf("Cannot read DAG run task (%s, %s, %s) from DB: %s",
			dagId, execTs, taskId, dbErr.Error())
	}
	return drt
}

// Initialize Scheduler for tests.
func schedulerWithSqlite(queues Queues, config Config, t *testing.T) *Scheduler {
	t.Helper()
	dbClient, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	return New(dbClient, queues, config)
}
