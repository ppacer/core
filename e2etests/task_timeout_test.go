package e2etests

import (
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/scheduler"
)

func TestSimpleDagRunWithTaskTimeout(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	notifications := make([]string, 0)
	dagId := dag.Id("mock_failing_dag")
	d := simpleDAGWithLongRunningTasks(
		dagId, schedule, 10*time.Hour, 100*time.Millisecond, &notifications,
	)
	dags.Add(d)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(
		dags, dr, 3*time.Second, false, &notifications, t,
	)

	if len(notifications) < 1 {
		t.Fatalf("Expected at least 1 notification")
	}
	const expectedNotification = "CONTEXT IS DONE"
	if notifications[0] != expectedNotification {
		t.Errorf("Expected notification [%s], got: [%s]", expectedNotification,
			notifications[0])
	}
}

func TestSimpleDagRunWithManyTaskTimeouts(t *testing.T) {
	// TODO!
}
