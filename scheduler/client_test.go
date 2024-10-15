// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
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
	cancel()
}

func TestClientTriggerDagRunSimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	cfg := DefaultConfig
	dags := dag.Registry{}
	dagId := "mock_dag"
	dags.Add(simpleDagNoSchedule(dagId))
	scheduler := schedulerWithSqlite(DefaultQueues(cfg), cfg, t)
	testServer := httptest.NewServer(scheduler.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	defer testServer.Close()
	defer cancel()

	schedClient := NewClient(
		testServer.URL, nil, testLogger(), DefaultClientConfig,
	)
	beforeTriggerTs := timeutils.Now()
	triggerInput := api.DagRunTriggerInput{DagId: dagId}
	err := schedClient.TriggerDagRun(triggerInput)
	if err != nil {
		t.Errorf("Error while triggering DAG run: %s", err.Error())
	}

	const maxWait = 1 * time.Second
	noDagruns := true
	for i := 0; i < 10; i++ {
		time.Sleep(maxWait / 10)
		if scheduler.dbClient.Count("dagruns") > 0 {
			noDagruns = false
			break
		}
	}
	if noDagruns {
		t.Fatalf("Expected at least one DAG run in the database, got none.")
	}
	drs, dbErr := scheduler.dbClient.ReadDagRuns(ctx, dagId, 1)
	if dbErr != nil {
		t.Errorf("Cannot read DAG runs from the database: %s", dbErr.Error())
	}
	t.Logf("DR: %+v", drs[0])
	execTs := timeutils.FromStringMust(drs[0].ExecTs)
	if execTs.Before(beforeTriggerTs) {
		t.Errorf("Expected execution timestamp after the trigger time, got: %s",
			execTs)
	}
}

func TestClientTriggerDagRunNonExistent(t *testing.T) {
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
	triggerInput := api.DagRunTriggerInput{DagId: "nonexistent_dag"}
	err := schedClient.TriggerDagRun(triggerInput)
	if err == nil {
		t.Fatalf("Expected non-nil error for triggering non-existent DAG")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected status code 404 in error, but found none: %s",
			err.Error())
	}
}

func TestClientRestartDagRunNonExistent(t *testing.T) {
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
	restartInput := api.DagRunRestartInput{
		DagId:  "nonexistent_dag",
		ExecTs: timeutils.ToString(time.Now()),
	}
	err := schedClient.RestartDagRun(restartInput)
	if err == nil {
		t.Fatalf("Expected non-nil error for triggering non-existent DAG")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected status code 404 in error, but found none: %s",
			err.Error())
	}
}

func simpleDagNoSchedule(dagId string) dag.Dag {
	n1 := dag.NewNode(printTask{Name: "t1"})
	n2 := dag.NewNode(printTask{Name: "t2"})
	n1.Next(n2)
	return dag.New(dag.Id(dagId)).AddRoot(n1).Done()
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

func TestClientUIDagrunDetailsEmpty(t *testing.T) {
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
	_, err := schedClient.UIDagrunDetails(42)
	if err == nil {
		t.Error("Expected non-nil error for reading details on empty database, but got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("Expected status code 400 in error, but found none: %s",
			err.Error())
	}
}

func TestClientUIDagrunDetailsRunning(t *testing.T) {
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
	execTs := timeutils.ToString(now)

	t1Name := "t1"
	t2Name := "t2"
	t3Name := "t3k"
	n1 := dag.NewNode(printTask{Name: t1Name})
	n2 := dag.NewNode(printTask{Name: t2Name})
	n3 := dag.NewNode(printTask{Name: t3Name})
	n1.Next(n2)
	n1.Next(n3)
	d := dag.New(dag.Id(dagId)).AddRoot(n1).Done()

	// Insert DAG tasks
	iErr := scheduler.dbClient.InsertDagTasks(ctx, d)
	if iErr != nil {
		t.Errorf("Cannot insert DAG tasks: %s", iErr.Error())
	}
	insertDagRun(
		scheduler.dbClient, ctx, dagId, execTs, dag.RunRunning.String(), t,
	)

	// Insert DAG run tasks
	for _, task := range d.Flatten() {
		if task.Id() == t3Name {
			continue
		}
		insertDagRunTask(
			scheduler.dbClient, ctx, dagId, execTs, task.Id(),
			dag.RunSuccess.String(), t,
		)
	}

	// Insert task logs
	const logMessage = "TEST LOG MESSAGE"
	for _, task := range d.Flatten() {
		if task.Id() == t3Name {
			continue
		}
		ti := tasklog.TaskInfo{
			DagId:  dagId,
			ExecTs: now,
			TaskId: task.Id(),
			Retry:  0,
		}
		tLogger := scheduler.taskLogs.GetLogger(ti)
		tLogger.Warn(logMessage)
	}

	drtDetails, err := schedClient.UIDagrunDetails(1)
	if err != nil {
		t.Errorf("Error while UIDagrunDetails: %s", err.Error())
	}
	if drtDetails.RunId != 1 {
		t.Errorf("Expected RunId=1, got: %d", drtDetails.RunId)
	}
	if drtDetails.DagId != dagId {
		t.Errorf("Expected DagId=%s, got: %s", dagId, drtDetails.DagId)
	}
	if drtDetails.Status != dag.RunRunning.String() {
		t.Errorf("Expected Status=%s, got: %s", dag.RunRunning.String(),
			drtDetails.Status)
	}
	if len(drtDetails.Tasks) != len(d.Flatten()) {
		t.Errorf("Expected %d tasks, got: %d", len(d.Flatten()),
			len(drtDetails.Tasks))
	}
	if drtDetails.ExecTsRaw != execTs {
		t.Errorf("Expected ExecTsRaw=%s, got: %s", execTs,
			drtDetails.ExecTsRaw)
	}
	if !drtDetails.Tasks[2].TaskNoStarted {
		t.Errorf("Task %s should be not started yet, but it is",
			drtDetails.Tasks[2].TaskId)
	}

	for _, task := range drtDetails.Tasks {
		if task.TaskId == t3Name {
			if len(task.TaskLogs.Records) != 0 {
				t.Errorf("Task %s shouldn't have any logs, but it does: %d",
					t3Name, len(task.TaskLogs.Records))
			}
			expPos := api.TaskPos{Depth: 2, Width: 2}
			if task.Pos != expPos {
				t.Errorf("Expected for task=%s position=%+v, got: %+v",
					t3Name, expPos, task.Pos)
			}
			continue
		}
		if task.Status != dag.TaskSuccess.String() {
			t.Errorf("Expected status=%s for task=%s, got: %s",
				dag.TaskSuccess.String(), task.TaskId, task.Status)
		}
		expDepth := 1
		if task.TaskId == t2Name {
			expDepth = 2
		}
		if task.Pos.Depth != expDepth {
			t.Errorf("Expected Pos.Depth=%d for task=%s, but got: %d",
				expDepth, task.TaskId, task.Pos.Depth)
		}
		if len(task.TaskLogs.Records) != 1 {
			t.Errorf("Expected 1 task log records for task=%s, got: %d",
				task.TaskId, len(task.TaskLogs.Records))
		}
		if task.TaskLogs.Records[0].Message != logMessage {
			t.Errorf("Expected, for task=%s, log message [%s], got: [%s]",
				task.TaskId, logMessage, task.TaskLogs.Records[0].Message)
		}
	}
}

func TestClientUIDagrunTaskDetailsEmpty(t *testing.T) {
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
	_, err := schedClient.UIDagrunTaskDetails(42, "sample_task", 0)
	if err == nil {
		t.Error("Expected error for loading DAG run tasks for empty DB, got nil")
	}
}

func TestClientUIDagrunTaskDetailsNoLogs(t *testing.T) {
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

	t1Name := "t1"
	t2Name := "t2"
	t3Name := "t3k"
	n1 := dag.NewNode(printTask{Name: t1Name})
	n2 := dag.NewNode(printTask{Name: t2Name})
	n3 := dag.NewNode(printTask{Name: t3Name})
	n1.Next(n2)
	n1.Next(n3)
	d := dag.New(dag.Id(dagId)).AddRoot(n1).Done()

	// Insert DAG tasks
	iErr := scheduler.dbClient.InsertDagTasks(ctx, d)
	if iErr != nil {
		t.Errorf("Cannot insert DAG tasks: %s", iErr.Error())
	}

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
	drtd, err := schedClient.UIDagrunTaskDetails(1, "t1", 0)
	if err != nil {
		t.Errorf("Unexpected error for UIDagrunStats: %s", err.Error())
	}
	logs := drtd.TaskLogs
	if logs.LogRecordsCount != 0 {
		t.Errorf("Expected 0 log records, got: %d", logs.LogRecordsCount)
	}
	if len(logs.Records) != 0 {
		t.Errorf("Expected 0 log records, got: %d", logs.LogRecordsCount)
	}
}

func TestClientUIDagrunTaskDetails(t *testing.T) {
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

	t1Name := "t1"
	t2Name := "t2"
	t3Name := "t3k"
	n1 := dag.NewNode(printTask{Name: t1Name})
	n2 := dag.NewNode(printTask{Name: t2Name})
	n3 := dag.NewNode(printTask{Name: t3Name})
	n1.Next(n2)
	n1.Next(n3)
	d := dag.New(dag.Id(dagId)).AddRoot(n1).Done()

	// Insert DAG tasks
	iErr := scheduler.dbClient.InsertDagTasks(ctx, d)
	if iErr != nil {
		t.Errorf("Cannot insert DAG tasks: %s", iErr.Error())
	}

	dagruns := []struct {
		execTs string
		status dag.RunStatus
	}{
		{timeutils.ToString(now), dag.RunScheduled},
	}

	tasks := []struct {
		execTs string
		taskId string
		status dag.TaskStatus
	}{
		{dagruns[0].execTs, "t1", dag.TaskSuccess},
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

	ti := tasklog.TaskInfo{
		DagId:  dagId,
		ExecTs: timeutils.FromStringMust(dagruns[0].execTs),
		TaskId: "t1",
		Retry:  0,
	}

	nLogs := 20
	taskLogger := scheduler.taskLogs.GetLogger(ti)
	for i := 0; i < nLogs; i++ {
		taskLogger.Warn("test message", "i", i)
	}

	// query stats
	drtd, err := schedClient.UIDagrunTaskDetails(1, "t1", 0)
	if err != nil {
		t.Errorf("Unexpected error for UIDagrunStats: %s", err.Error())
	}
	if drtd.TaskLogs.LogRecordsCount != nLogs {
		t.Errorf("Expected %d log records, got: %d", nLogs,
			drtd.TaskLogs.LogRecordsCount)
	}
	if len(drtd.TaskLogs.Records) != nLogs {
		t.Errorf("Expected %d log records, got: %d", nLogs,
			len(drtd.TaskLogs.Records))
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
	dbLogsClient, err := db.NewSqliteTmpClientForLogs(logger)
	if err != nil {
		t.Fatal(err)
	}
	notifier := notify.NewLogsErr(logger)
	logsFactory := tasklog.NewSQLite(dbLogsClient, nil)
	return New(dbClient, logsFactory, queues, config, logger, notifier)
}
