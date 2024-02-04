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

func TestSchedulerE2eSimpleDag(t *testing.T) {
	cfg := scheduler.DefaultConfig
	queues := scheduler.DefaultQueues(cfg)
	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule dag.Schedule = dag.FixedSchedule{Start: startTs, Interval: time.Hour}
	dagId := dag.Id("mock_dag_1")
	dags[dagId] = simple131DAG(dagId, &schedule)

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

	// Schedule new DAG run
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}
	scheduleNewDagRun(dbClient, queues, dr, t)

	waitForDagRunCompletion(dbClient, dr, 10*time.Millisecond, 3*time.Second, t)
}

func waitForDagRunCompletion(
	dbClient *db.Client, dr scheduler.DagRun, pollInterval, timeout time.Duration,
	t *testing.T,
) {
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
