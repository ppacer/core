// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package e2etests

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/exec"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

func TestSchedulerE2eSimpleDagEmptyTasks(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_dag_1")
	dags.Add(simple131DAG(dagId, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, t,
	)
}

func TestSchedulerE2eSimpleDagEmptyTasksNoSched(t *testing.T) {
	dags := dag.Registry{}
	dagId := dag.Id("mock_dag_1")
	dags.Add(simple131DAG(dagId, nil))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, t,
	)
}

func TestSchedulerE2eLinkedListEmptyTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_LL_1")
	const llSize = 10
	dags.Add(linkedListEmptyTasksDAG(dagId, llSize, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, t,
	)
}

func TestSchedulerE2eLinkedListWaitTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_LL_wait_1")
	const llSize = 10
	dags.Add(linkedListWaitTasksDAG(dagId, llSize, 1*time.Millisecond, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, t,
	)
}

func TestSchedulerE2eSimpleDagWithErrTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	dags.Add(simpleDAGWithErrTask(dagId, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, false, &notifications, t,
	)

	if len(notifications) == 0 {
		t.Error("Expected at least one error notification, but got zero")
	}
	/*
		t.Log("Notifications:")
		for _, n := range notifications {
			t.Log(n)
		}
	*/
}

func TestSchedulerE2eSimpleDagWithErrTaskCustomNotifier(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	notifications := make([]string, 0)
	notifierPhrase := "Who's mocking whom?"
	notifier := newCustomNotifier(notifierPhrase, &notifications)
	dagId := dag.Id("mock_failing_dag")
	dags.Add(simpleDAGWithErrTaskCustomNotifier(dagId, &schedule, notifier))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, false, &notifications, t,
	)

	if len(notifications) == 0 {
		t.Error("Expected at least one error notification, but got zero")
	}
	for idx, notification := range notifications {
		if !strings.Contains(notification, notifierPhrase) {
			t.Errorf("Notification [%d] [%s] does not contain expected phrase [%s]",
				idx, notification, notifierPhrase)
		}
	}
	/*
		t.Log("Notifications:")
		for _, n := range notifications {
			t.Log(n)
		}
	*/
}

func TestSchedulerE2eSimpleDagWithRuntimeErrTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	dags.Add(simpleDAGWithRuntimeErrTask(dagId, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, false, &notifications, t,
	)

	if len(notifications) == 0 {
		t.Error("Expected at least one error notification, but got zero")
	}
}

func TestSchedulerE2eTwoDagRunsSameTimeSameSchedule(t *testing.T) {
	dags := dag.Registry{}
	notifications := make([]string, 0)

	// dag1
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_dag_1")
	dags.Add(simple131DAG(dagId, &schedule))

	// dag2
	dagId2 := dag.Id("mock_LL_2")
	const llSize = 10
	dags.Add(linkedListEmptyTasksDAG(dagId2, llSize, &schedule))

	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	drs := []scheduler.DagRun{
		{DagId: dagId, AtTime: ts},
		{DagId: dagId2, AtTime: ts},
	}

	testSchedulerE2eManyDagRuns(
		dags, drs, 3*time.Second, true, &notifications, t,
	)
}

func TestSchedulerE2eWritingLogsToSQLite(t *testing.T) {
	cfg := scheduler.DefaultConfig
	queues := scheduler.DefaultQueues(cfg)
	notifications := make([]string, 0)
	notifier := notify.NewLogsErr(slog.Default())

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_dag_1")
	dags.Add(simpleLoggingDAG(dagId, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	// Start scheduler
	sched, dbClient, logsDbClient := schedulerWithSqlite(
		queues, cfg, &notifications, t,
	)
	testServer := httptest.NewServer(sched.Start(dags))
	defer testServer.Close()
	defer db.CleanUpSqliteTmp(dbClient, t)
	defer db.CleanUpSqliteTmp(logsDbClient, t)

	// Start executor
	go func() {
		executor := exec.New(testServer.URL, logsDbClient, nil, nil, notifier)
		executor.Start(dags)
	}()

	// Schedule new DAG runs
	scheduleNewDagRun(dbClient, queues, dr, t)

	// Wait for DAG run completion or timeout.
	const poll = 10 * time.Millisecond
	waitForDagRunCompletion(dbClient, dr, poll, 5*time.Second, true, t)

	// Test logs
	ctx := context.Background()
	tlrs, dbErr := logsDbClient.ReadDagRunLogs(
		ctx, string(dagId), timeutils.ToString(ts),
	)
	if dbErr != nil {
		t.Errorf("Error while reading logs from database: %s", dbErr.Error())
	}
	dagNodes := len(dags[dagId].Root.Flatten())
	if len(tlrs) != dagNodes {
		t.Errorf("Expected %d log records, got: %d", dagNodes, len(tlrs))
	}
}

// This test runs end-to-end test (scheduler and executor run in separate
// goroutines communicating via HTTP) for single DAG run.
func testSchedulerE2eSingleDagRun(
	dags dag.Registry, dr scheduler.DagRun, timeout time.Duration,
	expectSuccess bool, notifications *[]string, t *testing.T,
) {
	t.Helper()
	drs := []scheduler.DagRun{dr}
	testSchedulerE2eManyDagRuns(
		dags, drs, timeout, expectSuccess, notifications, t,
	)
}

func testSchedulerE2eManyDagRuns(
	dags dag.Registry, drs []scheduler.DagRun, timeout time.Duration,
	expectSuccess bool, notifications *[]string, t *testing.T,
) {
	t.Helper()
	cfg := scheduler.DefaultConfig
	queues := scheduler.DefaultQueues(cfg)
	notifier := notify.NewLogsErr(slog.Default())

	// Start scheduler
	sched, dbClient, logsDbClient := schedulerWithSqlite(
		queues, cfg, notifications, t,
	)
	testServer := httptest.NewServer(sched.Start(dags))
	defer testServer.Close()
	defer db.CleanUpSqliteTmp(dbClient, t)
	defer db.CleanUpSqliteTmp(logsDbClient, t)

	// Start executor
	go func() {
		executor := exec.New(testServer.URL, logsDbClient, nil, nil, notifier)
		executor.Start(dags)
	}()

	// Schedule new DAG runs
	for _, dr := range drs {
		scheduleNewDagRun(dbClient, queues, dr, t)
	}

	// Wait for DAG run completion or timeout.
	const poll = 10 * time.Millisecond
	for _, dr := range drs {
		waitForDagRunCompletion(dbClient, dr, poll, timeout, expectSuccess, t)
	}
}

func waitForDagRunCompletion(
	dbClient *db.Client, dr scheduler.DagRun, pollInterval, timeout time.Duration,
	testFailWhenRunFails bool, t *testing.T,
) {
	t.Helper()
	ctx := context.TODO()
	ticker := time.NewTicker(pollInterval)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			dagruns, err := dbClient.ReadLatestDagRuns(ctx)
			if err != nil {
				t.Errorf("Error while reading DAG runs: %s", err.Error())
			}
			drDb, exists := dagruns[string(dr.DagId)]
			if !exists {
				t.Errorf("DAG %s does not exist in LatestDagRuns",
					string(dr.DagId))
			}
			if drDb.Status == dag.RunFailed.String() {
				if testFailWhenRunFails {
					t.Fatalf("DAG run %+v finished with status FAILED", dr)
				}
				return
			}
			if drDb.Status == dag.RunSuccess.String() {
				return
			}
		case <-timeoutChan:
			t.Fatal("Time out! DAG run has not finished in time.")
			return
		}
	}
}

func scheduleNewDagRun(
	dbClient *db.Client, queues scheduler.Queues, dr scheduler.DagRun,
	t *testing.T,
) {
	t.Helper()
	ctx := context.Background()
	_, iErr := dbClient.InsertDagRun(
		ctx, string(dr.DagId), timeutils.ToString(dr.AtTime),
	)
	if iErr != nil {
		t.Fatalf("Error while inserting new DAG run: %s", iErr.Error())
	}
	queues.DagRuns.Put(dr)
}

func schedulerWithSqlite(
	queues scheduler.Queues, config scheduler.Config, notifications *[]string,
	t *testing.T,
) (*scheduler.Scheduler, *db.Client, *db.Client) {
	t.Helper()
	dbClient, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	logsDbClient, lErr := db.NewSqliteTmpClientForLogs(nil)
	if lErr != nil {
		t.Fatal(lErr)
	}
	notifier := notify.NewMock(notifications)
	sched := scheduler.New(dbClient, queues, config, nil, notifier)
	return sched, dbClient, logsDbClient
}
