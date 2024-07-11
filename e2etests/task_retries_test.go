// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package e2etests

import (
	"context"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

func TestSimpleDagRunWithRetries(t *testing.T) {
	const failedRuns = 3
	const maxRetries = 3

	var dbClient *db.Client = nil
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithTaskConfigFuncs(
		dagId, &schedule, failedRuns, dag.WithTaskRetries(maxRetries),
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, dbClient, t,
	)
}

func TestSimpleDagRunWithZeroRetries(t *testing.T) {
	var dbClient *db.Client = nil
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithTaskConfigFuncs(
		dagId, &schedule, 0, dag.WithTaskRetries(0), // the same as no retries
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, dbClient, t,
	)
}

func TestSimpleDagRunWithFailureAfterRetries(t *testing.T) {
	const failedRuns = 10
	const maxRetries = 3

	var dbClient *db.Client = nil
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithTaskConfigFuncs(
		dagId, &schedule, failedRuns, dag.WithTaskRetries(maxRetries),
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, false, &notifications, dbClient, t,
	)
}

func TestSimpleDagRunWithRetriesAndAlerts(t *testing.T) {
	const failedRuns = 3
	const maxRetries = 5

	var dbClient *db.Client = nil
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithTaskConfigFuncs(
		dagId, &schedule, failedRuns,
		dag.WithTaskRetries(maxRetries),
		dag.WithTaskSendAlertOnRetries,
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, dbClient, t,
	)

	if len(notifications) != failedRuns {
		t.Errorf("Expected %d notification on task retries, got: %d",
			failedRuns, len(notifications))

		t.Log("notifications:")
		for _, msg := range notifications {
			t.Logf("  -%s\n", msg)
		}
	}
}

func TestDagRunWithParallelRetries(t *testing.T) {
	var dbClient *db.Client = nil
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	dags.Add(simple131DagWithRetries(dagId, &schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, dbClient, t,
	)

	if len(notifications) == 0 {
		t.Error("Expected at least 1 notification on task retries, got: 0")
	}
}

func simple131DagWithRetries(dagId dag.Id, sched *schedule.Schedule) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "n1"})
	n21 := dag.NewNode(
		&failNTimesTask{taskId: "n21", n: 1},
		dag.WithTaskRetries(3),
		dag.WithTaskSendAlertOnRetries,
	)
	n22 := dag.NewNode(emptyTask{taskId: "n22"})
	n23 := dag.NewNode(
		&failNTimesTask{taskId: "n23", n: 4},
		dag.WithTaskRetries(5),
	)
	n3 := dag.NewNode(emptyTask{taskId: "n3"})
	n1.NextAsyncAndMerge([]*dag.Node{n21, n22, n23}, n3)

	d := dag.New(dagId)
	d.AddRoot(n1)
	d.AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func TestDagRunWithDelayedRetries(t *testing.T) {
	const failedRuns = 2
	const maxRetries = 5
	const taskIdWithRetries = "task1"
	delayBeforeRetry := 256 * time.Millisecond

	dbClient, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(dbClient, t)

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithTaskConfigFuncs(
		dagId, &schedule, failedRuns,
		dag.WithTaskRetries(maxRetries),
		dag.WithTaskRetriesDelay(delayBeforeRetry),
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	notifications := make([]string, 0)

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, true, &notifications, dbClient, t,
	)

	ctx := context.Background()
	drts, readErr := dbClient.ReadDagRunTasks(
		ctx, string(dagId), timeutils.ToString(ts),
	)
	if readErr != nil {
		t.Errorf("Could not read dag run tasks from the database: %s",
			readErr.Error())
	}
	expectedNumOfTasks := failedRuns + 1 + 2
	if len(drts) != expectedNumOfTasks {
		t.Errorf("Expected %d tasks in the dagruntasks table, got: %d",
			expectedNumOfTasks, len(drts))
	}
	testGapDurationBetweenTasks(drts, taskIdWithRetries, delayBeforeRetry, t)
}

func testGapDurationBetweenTasks(
	drts []db.DagRunTask, taskId string, expectedDelay time.Duration,
	t *testing.T,
) {
	t.Helper()
	if len(drts) <= 1 {
		t.Fatalf("Expected at least two DAG run tasks, got: %d", len(drts))
	}
	insertTimestamps := make([]time.Time, 0, len(drts))
	for _, drt := range drts {
		if drt.TaskId != taskId {
			continue
		}
		ts, castErr := timeutils.FromString(drt.InsertTs)
		if castErr != nil {
			t.Errorf("Cannot convert %s into time.Time: %s", drt.InsertTs,
				castErr.Error())
		}
		insertTimestamps = append(insertTimestamps, ts)
	}
	if len(insertTimestamps) <= 1 {
		t.Fatalf("Expected at least two records for taskId=%s, but got: %d",
			taskId, len(insertTimestamps))
	}

	prevTs := insertTimestamps[0]
	for idx := 1; idx < len(insertTimestamps); idx++ {
		ts := insertTimestamps[idx]
		timeDiff := ts.Sub(prevTs)
		if timeDiff < expectedDelay {
			t.Errorf("Expected at least %v delay, got %v for prev=%v current=%v",
				expectedDelay, timeDiff, prevTs, ts)
		}
		prevTs = ts
	}
}
