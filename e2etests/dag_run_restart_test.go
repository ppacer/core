package e2etests

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/exec"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/scheduler"
)

func TestDagRunTrigger(t *testing.T) {
	const dagId = "sample_dag"
	timeout := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)

	logs := make([]string, 0, 100)
	sw := newSliceWriter(&logs)
	logger := sliceLogger(sw)

	defer cancel()
	cfg := scheduler.DefaultConfig
	notifications := []string{}
	dags := dag.Registry{}
	dags.Add(simple131DAG(dag.Id(dagId), nil))

	sched, dbClient, logsDbClient := schedulerWithSqlite(
		scheduler.DefaultQueues(cfg), cfg, &notifications, nil, nil, logger, t,
	)
	testServer := httptest.NewServer(sched.Start(ctx, dags))
	defer db.CleanUpSqliteTmp(dbClient, t)
	defer db.CleanUpSqliteTmp(logsDbClient, t)
	defer testServer.Close()

	schedClient := scheduler.NewClient(
		testServer.URL, nil, nil, scheduler.ClientConfig{
			HttpClientTimeout: 3 * time.Second,
		},
	)

	doneChan := make(chan struct{})
	inputChan := make(chan expLatestDagRunStatus)
	outputChan := make(chan error)

	go runExecAndCheckDagRuns(
		dags, testServer.URL, dbClient, logsDbClient, timeout, &notifications,
		logger, doneChan, inputChan, outputChan, t,
	)

	err := schedClient.TriggerDagRun(api.DagRunTriggerInput{DagId: dagId})
	if err != nil {
		t.Errorf("Error while triggering DAG run: %v", err)
	}
	inputChan <- expLatestDagRunStatus{
		DagId:  dagId,
		Status: dag.RunSuccess.String(),
	}

	outErr1 := <-outputChan
	if outErr1 != nil {
		t.Fatalf("Triggered DAG run error: %s", outErr1.Error())
	}

	exposeSliceLoggerOnTestFailure(logs, t)
	doneChan <- struct{}{}
}

func TestWholeDagRunRestarts(t *testing.T) {
	f := func(taskRetries, failedTaskRetries int) dag.Dag {
		return simpleDAGWithTaskConfigFuncs(
			"failing_dag", nil, failedTaskRetries,
			dag.WithTaskRetries(taskRetries),
		)
	}

	testCases := []struct {
		name          string
		d             dag.Dag
		dagRunRetries int
	}{
		{"no_restarts_needed", f(5, 1), 0},
		{"single_dag_run_restart", f(3, 5), 1},
		{"two_dag_run_restarts", f(3, 7), 2},
		{"two_dag_run_restarts_upper", f(3, 9), 2},
		{"ten_dag_run_restarts", f(1, 11), 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, generateDagRunRestartTest(tc.d, tc.dagRunRetries))
	}
}

// initial run isn't included in the retries
func generateDagRunRestartTest(
	d dag.Dag, dagRunRetries int,
) func(*testing.T) {
	return func(t *testing.T) {
		dagId := string(d.Id)
		timeout := 5 * time.Second
		ctx, cancel := context.WithTimeout(
			context.Background(), defaultTimeout,
		)

		logs := make([]string, 0, 100)
		sw := newSliceWriter(&logs)
		logger := sliceLogger(sw)

		defer cancel()
		cfg := scheduler.DefaultConfig
		notifications := []string{}
		dags := dag.Registry{}
		dags.Add(d)

		sched, dbClient, logsDbClient := schedulerWithSqlite(
			scheduler.DefaultQueues(cfg), cfg, &notifications, nil, nil,
			logger, t,
		)
		testServer := httptest.NewServer(sched.Start(ctx, dags))
		defer db.CleanUpSqliteTmp(dbClient, t)
		defer db.CleanUpSqliteTmp(logsDbClient, t)
		defer testServer.Close()

		schedClient := scheduler.NewClient(
			testServer.URL, nil, nil, scheduler.ClientConfig{
				HttpClientTimeout: 3 * time.Second,
			},
		)

		doneChan := make(chan struct{})
		inputChan := make(chan expLatestDagRunStatus)
		outputChan := make(chan error)

		go runExecAndCheckDagRuns(
			dags, testServer.URL, dbClient, logsDbClient, timeout,
			&notifications, logger, doneChan, inputChan, outputChan, t,
		)

		// Initial DAG run
		err := schedClient.TriggerDagRun(api.DagRunTriggerInput{DagId: dagId})
		if err != nil {
			t.Errorf("Error while triggering DAG run: %v", err)
		}
		expStatus := dag.RunFailed.String()
		if dagRunRetries == 0 {
			expStatus = dag.RunSuccess.String()
		}
		inputChan <- expLatestDagRunStatus{
			DagId:  dagId,
			Status: expStatus,
		}
		outErr1 := <-outputChan
		if outErr1 != nil {
			t.Fatalf("First DAG run error: %s", outErr1.Error())
		}
		drLatest, dbErr1 := dbClient.ReadLatestDagRuns(ctx)
		if dbErr1 != nil {
			t.Fatalf("Cannot read LatestDagRuns: %s", dbErr1.Error())
		}
		if _, exists := drLatest[dagId]; !exists {
			t.Fatalf("DAG run %s not found in LatestDagRuns map", dagId)
		}
		execTs := drLatest[dagId].ExecTs
		in := api.DagRunRestartInput{
			DagId:  dagId,
			ExecTs: execTs,
		}

		for retryId := 1; retryId <= dagRunRetries; retryId++ {
			retryErr := schedClient.RestartDagRun(in)
			if retryErr != nil {
				t.Errorf("Cannot retry (%d) DAG run (%s: %s): %s", retryId,
					dagId, execTs, retryErr.Error())
			}
			expStatus := dag.RunFailed.String()
			if retryId == dagRunRetries {
				expStatus = dag.RunSuccess.String()
			}
			inputChan <- expLatestDagRunStatus{
				DagId:  dagId,
				Status: expStatus,
			}
			outErr := <-outputChan
			if outErr != nil {
				t.Fatalf("The second DAG run error: %s", outErr.Error())
			}
		}

		dbRestartCnt := dbClient.Count("dagrunrestarts")
		if dbRestartCnt != dagRunRetries {
			t.Fatalf("Expected %d dag run restarts in dagrunrestarts table, got %d",
				dagRunRetries, dbRestartCnt)
		}

		exposeSliceLoggerOnTestFailure(logs, t)
		doneChan <- struct{}{}
	}
}

type expLatestDagRunStatus struct {
	DagId  string
	Status string
}

func runExecAndCheckDagRuns(
	dags dag.Registry,
	testServerUrl string,
	dbClient *db.Client,
	logsDbClient *db.LogsClient,
	timeout time.Duration,
	notifications *[]string,
	logger *slog.Logger,
	doneChan chan struct{},
	inputDRStatusChan chan expLatestDagRunStatus,
	outputErrChan chan error,
	t *testing.T,
) {
	t.Helper()

	// Start executor
	notifier := notify.NewMock(notifications)
	go func() {
		executor := exec.New(
			testServerUrl, logsDbClient, nil, logger, nil, notifier,
		)
		executor.Start(dags)
	}()
	timeoutChan := time.After(timeout)

	for {
		select {
		case <-timeoutChan:
			t.Fatalf("Timeout while waiting for DAG run status")
			return
		case <-doneChan:
			return
		case in := <-inputDRStatusChan:
			go waitForLatestDagRunStatus(
				dbClient, in.DagId, 10*time.Millisecond, timeout, in.Status,
				outputErrChan, t,
			)
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func waitForLatestDagRunStatus(
	dbClient *db.Client, dagId string, pollInterval, timeout time.Duration,
	expectedStatus string, outputChan chan error, t *testing.T,
) {
	ctx := context.TODO()
	ticker := time.NewTicker(pollInterval)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			dagruns, err := dbClient.ReadLatestDagRuns(ctx)
			if err != nil {
				t.Errorf("Error while reading DAG runs: %v", err)
				outputChan <- fmt.Errorf("error while reading DAG runs: %w", err)
				return
			}
			drDb, exists := dagruns[dagId]
			if !exists {
				// DAG run not yet created
				t.Logf("DAG run (%s) not yet created\n", dagId)
				continue
			}
			if drDb.Status == expectedStatus {
				outputChan <- nil
				return
			}
			status := drDb.Status
			failed := dag.RunFailed.String()
			success := dag.RunSuccess.String()
			if status != expectedStatus && (status == failed || status == success) {
				outputChan <- fmt.Errorf("DAG run (%s: %s) is in unexpected terminal state: %s",
					drDb.DagId, drDb.ExecTs, drDb.Status)
				return
			}
		case <-timeoutChan:
			outputChan <- fmt.Errorf("Timeout while waiting for DAG run status")
			return
		}
	}
}
