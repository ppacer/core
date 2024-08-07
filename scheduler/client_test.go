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

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

const defaultTimeout time.Duration = 10 * time.Second

func TestClientGetTaskEmptyDb(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	_, err := schedClient.GetTask()
	if err == nil {
		t.Error("Expected non-nil error for GetTask on empty database")
	}
	if err != ds.ErrQueueIsEmpty {
		t.Errorf("Expected empty queue error, but got: %s", err.Error())
	}
}

func TestClientGetTaskMockSingle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	queues := DefaultQueues(cfg)
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(queues, cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

	drt := DagRunTask{dag.Id("mock_dag"), time.Now(), "task1", 0}
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	queues := DefaultQueues(cfg)
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(queues, cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()
	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

	dagId := dag.Id("mock_dag")
	data := []DagRunTask{
		{dagId, time.Now(), "task1", 0},
		{dagId, time.Now(), "task2", 0},
		{dagId, time.Now(), "task3", 0},
		{dagId, time.Now(), "task4", 0},
		{dagId, time.Now(), "task5", 0},
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	tte := api.TaskToExec{
		DagId:  "mock_dag",
		ExecTs: timeutils.ToString(time.Now()),
		TaskId: "task1",
	}
	uErr := schedClient.UpsertTaskStatus(tte, dag.TaskSuccess, nil)
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()
	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

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
			0, initStatus)
		if iErr != nil {
			t.Errorf("Cannot insert DAG run task into DB: %s", iErr.Error())
		}
	}

	// Update task statuses to SUCCESS
	for _, taskId := range taskIds {
		tte := newTaskToExec(dagId, execTs, taskId)
		uErr := schedClient.UpsertTaskStatus(tte, newStatus, nil)
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()
	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

	const dagId = "mock_dag"
	execTs := time.Now()
	execTsStr := timeutils.ToString(execTs)
	const taskId = "mock_task_id"
	tte := newTaskToExec(dagId, execTsStr, taskId)

	u1Err := schedClient.UpsertTaskStatus(tte, dag.TaskScheduled, nil)
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
		uErr := schedClient.UpsertTaskStatus(tte, status, nil)
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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	schedState, err := schedClient.GetState()
	if err != nil {
		t.Errorf("Error when getting scheduler state: %s", err.Error())
	}
	if schedState != StateStarted && schedState != StateSynchronizing &&
		schedState != StateRunning {
		t.Errorf("Got unexpected Scheduler State: %s", schedState.String())
	}

	scheduler.setState(StateStopping)
	schedState2, err2 := schedClient.GetState()
	if err2 != nil {
		t.Errorf("Error when getting scheduler state for the second time: %s",
			err2.Error())
	}
	if schedState2 != StateStopping {
		t.Errorf("Expected Scheduled State %s, got: %s",
			StateStopping.String(), schedState2.String())
	}
}

func TestClientUIDagrunStatsEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	stats, err := schedClient.UIDagrunStats()
	if err != nil {
		t.Errorf("Expected non-nil error for UIDagrunStats on empty database, got: %s",
			err.Error())
	}
	var emptyStats api.StatusCounts
	if stats.Dagruns != emptyStats {
		t.Errorf("Expected empty api.UIDagrunStats.Dagruns, got: %v",
			stats.Dagruns)
	}
	if stats.DagrunTasks != emptyStats {
		t.Errorf("Expected empty api.UIDagrunStats.DagrunTasks, got: %v",
			stats.DagrunTasks)
	}
}

func TestClientUIDagrunStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

	// prep data
	const dagId = "sample_dag"
	now := time.Now()

	dagruns := []struct {
		execTs string
		status dag.RunStatus
	}{
		{timeutils.ToString(now), dag.RunScheduled},
		{timeutils.ToString(now.Add(13 * time.Hour)), dag.RunRunning},
	}

	tasks := []struct {
		execTs string
		taskId string
		status dag.TaskStatus
	}{
		{dagruns[1].execTs, "t1", dag.TaskSuccess},
		{dagruns[1].execTs, "t2", dag.TaskSuccess},
		{dagruns[1].execTs, "t3", dag.TaskRunning},
	}

	for _, dr := range dagruns {
		insertDagRun(
			scheduler.dbClient, ctx, dagId, dr.execTs, dr.status.String(), t,
		)
	}

	for _, task := range tasks {
		insertDagRunTask(
			scheduler.dbClient, ctx, dagId, task.execTs, task.taskId,
			task.status.String(), t,
		)
	}

	// query stats
	stats, err := schedClient.UIDagrunStats()
	if err != nil {
		t.Errorf("Unexpected error for UIDagrunStats: %s", err.Error())
	}

	expectedDagruns := api.StatusCounts{Scheduled: 1, Running: 1}
	if stats.Dagruns != expectedDagruns {
		t.Errorf("Expected stats for DAG runs %+v, but got: %+v",
			expectedDagruns, stats.Dagruns)
	}

	expectedDagrunTasks := api.StatusCounts{Success: 2, Running: 1}
	if stats.DagrunTasks != expectedDagrunTasks {
		t.Errorf("Expected stats for DAG run tasks %+v, but got: %+v",
			expectedDagrunTasks, stats.DagrunTasks)
	}
}

func TestClientUIDagrunLatestEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	dagruns1, err1 := schedClient.UIDagrunLatest(10)
	if err1 != nil {
		t.Errorf("Expected non-nil error for UIDagrunLatest on empty database, got: %s",
			err1.Error())
	}
	dagruns2, err2 := schedClient.UIDagrunLatest(1)
	if err2 != nil {
		t.Errorf("Expected non-nil error for UIDagrunLatest on empty database, got: %s",
			err2.Error())
	}
	if len(dagruns1) != 0 {
		t.Errorf("Expected 0 dagruns on empty DB, got: %d", len(dagruns1))
	}
	if len(dagruns2) != 0 {
		t.Errorf("Expected 0 dagruns on empty DB, got: %d", len(dagruns2))
	}
}

func TestClientUIDagrunLatest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)

	// prep data
	const dagId = "sample_dag"
	now := time.Now()

	dagruns := []struct {
		execTs string
		status dag.RunStatus
	}{
		{timeutils.ToString(now), dag.RunScheduled},
		{timeutils.ToString(now.Add(13 * time.Hour)), dag.RunRunning},
	}

	tasks := []struct {
		execTs string
		taskId string
		status dag.TaskStatus
	}{
		{dagruns[1].execTs, "t1", dag.TaskSuccess},
		{dagruns[1].execTs, "t2", dag.TaskSuccess},
		{dagruns[1].execTs, "t3", dag.TaskRunning},
	}

	for _, dr := range dagruns {
		insertDagRun(
			scheduler.dbClient, ctx, dagId, dr.execTs, dr.status.String(), t,
		)
	}

	for _, task := range tasks {
		insertDagRunTask(
			scheduler.dbClient, ctx, dagId, task.execTs, task.taskId,
			task.status.String(), t,
		)
	}

	// query stats
	drLatest, err := schedClient.UIDagrunLatest(10)
	if err != nil {
		t.Errorf("Unexpected error for UIDagrunLatest: %s", err.Error())
	}

	if len(drLatest) != len(dagruns) {
		t.Errorf("Expected %d latest dag runs, got: %d", len(dagruns),
			len(drLatest))
	}
	if drLatest[0].TaskNum != 0 {
		t.Errorf("Expected TaskNum 0 (no dagatsks were inserted), got: %+v",
			drLatest[0])
	}
	if drLatest[0].Status != dag.RunRunning.String() {
		t.Errorf("Expected DAG run to be %s, but is %s",
			dag.RunRunning.String(), drLatest[0].Status)
	}
	if drLatest[1].Status != dag.RunScheduled.String() {
		t.Errorf("Expected DAG run to be %s, but is %s",
			dag.RunScheduled.String(), drLatest[1].Status)
	}
}

func taskToExecEqualsDRT(
	task api.TaskToExec, expectedDrt DagRunTask, t *testing.T,
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

func newTaskToExec(dagId, execTs, taskId string) api.TaskToExec {
	return api.TaskToExec{
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
	drt, dbErr := dbClient.ReadDagRunTask(ctx, dagId, execTs, taskId, 0)
	if dbErr != nil {
		t.Errorf("Cannot read DAG run task (%s, %s, %s) from DB: %s",
			dagId, execTs, taskId, dbErr.Error())
	}
	return drt
}

func insertDagRun(
	dbClient *db.Client, ctx context.Context, dagId, execTs, status string,
	t *testing.T,
) {
	runId, iErr := dbClient.InsertDagRun(ctx, dagId, execTs)
	if iErr != nil {
		t.Fatalf("Cannot insert new DAG run: %s", iErr.Error())
	}
	uErr := dbClient.UpdateDagRunStatus(ctx, runId, status)
	if uErr != nil {
		t.Errorf("Cannot update DAG run (%d) status: %s", runId,
			uErr.Error())
	}
}

// Initialize Scheduler for tests.
func schedulerWithSqlite(queues Queues, config Config, t *testing.T) *Scheduler {
	t.Helper()
	logger := testLogger()
	dbClient, err := db.NewSqliteTmpClient(logger)
	if err != nil {
		t.Fatal(err)
	}
	notifier := notify.NewLogsErr(logger)
	return New(dbClient, queues, config, logger, notifier)
}
