package e2etests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

func TestSimpleDagRunWithTaskTimeout(t *testing.T) {
	logs := make([]string, 0, 100)
	sw := newSliceWriter(&logs)
	logger := sliceLogger(sw)

	dbClient, err := db.NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	logsDbClient, err := db.NewSqliteTmpClientForLogs(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(dbClient, t)
	defer db.CleanUpSqliteTmp(logsDbClient, t)

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	notifications := make([]string, 0)
	dagId := dag.Id("timeouted_dag")
	d := simpleDAGWithLongRunningTasks(
		dagId, schedule, 10*time.Hour, 100*time.Millisecond, &notifications,
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRunCustom(
		dags, dr, 3*time.Second, false, &notifications, dbClient, logsDbClient,
		nil, nil, logger, true, t,
	)

	if len(notifications) < 1 {
		t.Fatalf("Expected at least 1 notification")
	}
	const expectedNotification = "CONTEXT IS DONE"
	if notifications[0] != expectedNotification {
		t.Errorf("Expected notification [%s], got: [%s]", expectedNotification,
			notifications[0])
	}

	taskId := "task1"
	dbLogs, logsReadErr := logsDbClient.ReadDagRunTaskLogs(
		context.TODO(), string(dagId), timeutils.ToString(ts), taskId, 0,
	)
	if logsReadErr != nil {
		t.Errorf("Cannot read task logs from the database: %s",
			logsReadErr.Error())
	}
	if len(dbLogs) < 1 {
		t.Fatalf("Expected at least 1 task log record, got: %d", len(dbLogs))
	}

	timeoutInTaskMsgs := false
	for _, dbl := range dbLogs {
		if strings.Contains(dbl.Message, "exceeded timeout") {
			timeoutInTaskMsgs = true
			break
		}
	}
	if !timeoutInTaskMsgs {
		t.Error("Expected timeout error in task logs, but couldn't find. Logs:")
		for _, dbl := range dbLogs {
			t.Logf("  -%s: %s\n", dbl.InsertTs, dbl.Message)
		}
	}
}

func TestSimpleDagRunWithManyTaskTimeouts(t *testing.T) {
	const N = 10
	logs := make([]string, 0, 100)
	sw := newSliceWriter(&logs)
	logger := sliceLogger(sw)

	dbClient, err := db.NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	logsDbClient, err := db.NewSqliteTmpClientForLogs(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(dbClient, t)
	defer db.CleanUpSqliteTmp(logsDbClient, t)

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	notifications := make([]string, 0)
	dagId := dag.Id("many_timeouts_dag")
	d := manyParallelLongRunningsTasksDag(
		N, dagId, schedule, 10*time.Hour, 100*time.Millisecond, &notifications,
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRunCustom(
		dags, dr, 3*time.Second, false, &notifications, dbClient, logsDbClient,
		nil, nil, logger, true, t,
	)

	if len(notifications) < N {
		t.Fatalf("Expected at least %d notification", N)
	}
	const expectedNotification = "CONTEXT IS DONE"
	contextIsDoneNotifications := 0

	for _, notification := range notifications {
		if notification == expectedNotification {
			contextIsDoneNotifications++
		}
	}

	if contextIsDoneNotifications < N {
		t.Errorf("Expected %d notifications of the form [%s], got: %d",
			N, expectedNotification, contextIsDoneNotifications)
		t.Log("All notifications:")
		for _, notification := range notifications {
			t.Log(notification)
		}
	}

	tlrs, logsReadErr := logsDbClient.ReadDagRunLogs(
		context.TODO(), string(dagId), timeutils.ToString(ts),
	)
	if logsReadErr != nil {
		t.Errorf("Cannot read task logs from the database: %s",
			logsReadErr.Error())
	}
	if len(tlrs) < N {
		t.Fatalf("Expected at least %d task log record, got: %d", N,
			len(tlrs))
	}

	timeoutsInTaskLogs := 0
	for _, dbl := range tlrs {
		if strings.Contains(dbl.Message, "exceeded timeout") {
			timeoutsInTaskLogs++
		}
	}
	if timeoutsInTaskLogs < N {
		t.Errorf("Expected timeout error in %d task logs, but got in %d",
			N, timeoutsInTaskLogs)
		t.Log("Task log records:")
		for _, dbl := range tlrs {
			t.Logf("  -%s: %s\n", dbl.InsertTs, dbl.Message)
		}
	}
}
