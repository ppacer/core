package scheduler

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
)

func TestSchedulerStartCancellation(t *testing.T) {
	const timeout = 100 * time.Millisecond
	notifications := make([]string, 0)
	sched := defaultScheduler(&notifications, 10, t)
	defer db.CleanUpSqliteTmp(sched.dbClient, t)
	dags := dag.Registry{}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	testServer := httptest.NewServer(sched.Start(ctx, dags))
	defer cancel()
	defer testServer.Close()

	client := NewClient(testServer.URL, nil, testLogger(), DefaultClientConfig)
	_, err := client.GetState()
	if err != nil {
		t.Errorf("Cannot get Scheduler state on the first try: %s",
			err.Error())
	}

	time.Sleep(timeout)
	stateAfter, errAfter := client.GetState()
	if errAfter != nil {
		t.Errorf("Cannot get Scheduler state on the second try: %s",
			errAfter.Error())
	}
	if stateAfter != StateStopping {
		t.Errorf("Expected Scheduler to be in state %s, but it's %s",
			StateStopping.String(), stateAfter.String())
	}
}

func defaultScheduler(notifications *[]string, queueCap int, t *testing.T) *Scheduler {
	c, err := db.NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}

	drQueue := ds.NewSimpleQueue[DagRun](queueCap)
	taskQueue := ds.NewSimpleQueue[DagRunTask](queueCap)
	queues := Queues{
		DagRuns:     &drQueue,
		DagRunTasks: &taskQueue,
	}
	notifier := notify.NewMock(notifications)

	return New(
		c, queues, DefaultConfig, testLogger(), notifier,
	)
}
