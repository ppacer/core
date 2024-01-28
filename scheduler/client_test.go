package scheduler

import (
	"net/http/httptest"
	"testing"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
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

// Initialize test HTTP server for running Scheduler.
func schedulerTestServer(
	dags dag.Registry, queues Queues, config Config, t *testing.T,
) *httptest.Server {
	scheduler := schedulerTmp(queues, config, t)
	defer db.CleanUpSqliteTmp(scheduler.dbClient, t)
	handler := scheduler.Start(dags)
	return httptest.NewServer(handler)
}

// Initialize Scheduler for tests.
func schedulerTmp(queues Queues, config Config, t *testing.T) *Scheduler {
	dbClient, err := db.NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	return New(dbClient, queues, config)
}
