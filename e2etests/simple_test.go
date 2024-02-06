package e2etests

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/exec"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

func TestSchedulerE2eSimpleDagEmptyTasks(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule dag.Schedule = dag.FixedSchedule{Start: startTs, Interval: time.Hour}
	dagId := dag.Id("mock_dag_1")
	dags[dagId] = simple131DAG(dagId, &schedule)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(dags, dr, 3*time.Second, t)
}

func TestSchedulerE2eSimpleDagEmptyTasksNoSched(t *testing.T) {
	dags := dag.Registry{}
	dagId := dag.Id("mock_dag_1")
	dags[dagId] = simple131DAG(dagId, nil)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(dags, dr, 3*time.Second, t)
}

func TestSchedulerE2eLinkedListEmptyTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule dag.Schedule = dag.FixedSchedule{Start: startTs, Interval: time.Hour}
	dagId := dag.Id("mock_LL_1")
	const llSize = 10
	dags[dagId] = linkedListEmptyTasksDAG(dagId, llSize, &schedule)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(dags, dr, 3*time.Second, t)
}

func TestSchedulerE2eLinkedListWaitTask(t *testing.T) {
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule dag.Schedule = dag.FixedSchedule{Start: startTs, Interval: time.Hour}
	dagId := dag.Id("mock_LL_wait_1")
	const llSize = 10
	dags[dagId] = linkedListWaitTasksDAG(dagId, llSize, 1*time.Millisecond, &schedule)
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	testSchedulerE2eSingleDagRun(dags, dr, 3*time.Second, t)
}

func TestSchedulerE2eTwoDagRunsSameTimeSameSchedule(t *testing.T) {
	dags := dag.Registry{}

	// dag1
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule dag.Schedule = dag.FixedSchedule{Start: startTs, Interval: time.Hour}
	dagId := dag.Id("mock_dag_1")
	dags[dagId] = simple131DAG(dagId, &schedule)

	// dag2
	dagId2 := dag.Id("mock_LL_2")
	const llSize = 10
	dags[dagId2] = linkedListEmptyTasksDAG(dagId, llSize, &schedule)

	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	drs := []scheduler.DagRun{
		{DagId: dagId, AtTime: ts},
		{DagId: dagId2, AtTime: ts},
	}

	testSchedulerE2eManyDagRuns(dags, drs, 3*time.Second, t)
}

// This test runs end-to-end test (scheduler and executor run in separate
// goroutines communicating via HTTP) for single DAG run.
func testSchedulerE2eSingleDagRun(
	dags dag.Registry, dr scheduler.DagRun, timeout time.Duration, t *testing.T,
) {
	t.Helper()
	drs := []scheduler.DagRun{dr}
	testSchedulerE2eManyDagRuns(dags, drs, timeout, t)
}

func testSchedulerE2eManyDagRuns(
	dags dag.Registry, drs []scheduler.DagRun, timeout time.Duration, t *testing.T,
) {
	t.Helper()
	cfg := scheduler.DefaultConfig
	queues := scheduler.DefaultQueues(cfg)

	// Start scheduler
	sched, dbClient := schedulerWithSqlite(queues, cfg, t)
	testServer := httptest.NewServer(sched.Start(dags))
	defer testServer.Close()
	defer db.CleanUpSqliteTmp(dbClient, t)

	// Start executor
	go func() {
		executor := exec.New(testServer.URL, nil)
		executor.Start(dags)
	}()

	// Schedule new DAG runs
	for _, dr := range drs {
		scheduleNewDagRun(dbClient, queues, dr, t)
	}

	// Wait for DAG run completion or timeout.
	const poll = 10 * time.Millisecond
	for _, dr := range drs {
		waitForDagRunCompletion(dbClient, dr, poll, timeout, t)
	}
}

func waitForDagRunCompletion(
	dbClient *db.Client, dr scheduler.DagRun, pollInterval, timeout time.Duration,
	t *testing.T,
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
				t.Fatalf("DAG run %+v finished with status FAILED", dr)
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
	queues scheduler.Queues, config scheduler.Config, t *testing.T,
) (*scheduler.Scheduler, *db.Client) {
	t.Helper()
	dbClient, err := db.NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	return scheduler.New(dbClient, queues, config), dbClient
}
