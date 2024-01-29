package scheduler

import (
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
	testServer := schedulerTestServer(dags, DefaultQueues(cfg), cfg, t)

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
	testServer := schedulerTestServer(dags, queues, cfg, t)

	schedClient := NewClient(testServer.URL, nil, DefaultClientConfig)

	drt := DagRunTask{dag.Id("mock_dag"), time.Now(), "task1"}
	putErr := queues.DagRunTasks.Put(drt)
	if putErr != nil {
		t.Errorf("Cannot put new DAG run task onto the queue: %s",
			putErr.Error())
	}

	task, err := schedClient.GetTask()
	if err != nil {
		t.Errorf("Unexpected error for GetTask(): %s", err.Error())
	}
	taskToExecEqualsDRT(task, drt, t)

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
	testServer := schedulerTestServer(dags, queues, cfg, t)
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

	// Get and verify tasks
	for _, drt := range data {
		task, gtErr := schedClient.GetTask()
		if gtErr != nil {
			t.Errorf("Cannot GetTask(): %s", gtErr.Error())
		}
		taskToExecEqualsDRT(task, drt, t)
	}
}

// TODO: Add tests for UpdateTaskStatus

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

// Initialize test HTTP server for running Scheduler.
func schedulerTestServer(
	dags dag.Registry, queues Queues, config Config, t *testing.T,
) *httptest.Server {
	t.Helper()
	scheduler := schedulerTmp(queues, config, t)
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	handler := scheduler.Start(dags)
	return httptest.NewServer(handler)
}

// Initialize Scheduler for tests.
func schedulerTmp(queues Queues, config Config, t *testing.T) *Scheduler {
	t.Helper()
	dbClient, err := db.NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	return New(dbClient, queues, config)
}
