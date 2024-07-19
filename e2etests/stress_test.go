package e2etests

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/exec"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/scheduler"
)

func TestMaxGoroutineManyParallelTasks(t *testing.T) {
	const tasksCount = 50
	const maxGoroutines = 20
	cfg := scheduler.DefaultConfig
	cfg.TaskSchedulerConfig.MaxGoroutineCount = maxGoroutines

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("many_parallel_tasks_dag")
	dags.Add(manyParallelWaitTasksDag(dagId, schedule, tasksCount))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	drs := []scheduler.DagRun{dr}
	testMaxGoroutineCount(dags, drs, cfg, maxGoroutines, 30*time.Second, nil, t)
}

func TestMaxGoroutineManyParallelTasksFewDagRuns(t *testing.T) {
	const dagRuns = 4
	const tasksCount = 30
	const maxGoroutines = 15
	cfg := scheduler.DefaultConfig
	cfg.TaskSchedulerConfig.MaxGoroutineCount = maxGoroutines

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("many_parallel_tasks_dag")
	dags.Add(manyParallelWaitTasksDag(dagId, schedule, tasksCount))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)

	drs := make([]scheduler.DagRun, dagRuns)
	for i := 0; i < dagRuns; i++ {
		dr := scheduler.DagRun{
			DagId:  dagId,
			AtTime: ts.Add(time.Duration(i) * 13 * 60 * time.Minute),
		}
		drs[i] = dr
	}

	testMaxGoroutineCount(dags, drs, cfg, maxGoroutines+dagRuns,
		10*time.Second, nil, t)
}

func TestMaxGoroutineManyDagRunsSingleTask(t *testing.T) {
	const dagRuns = 50
	const maxGoroutines = 10
	cfg := scheduler.DefaultConfig
	cfg.TaskSchedulerConfig.MaxGoroutineCount = maxGoroutines

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("single_task_dag")
	dags.Add(singleEmptyTaskDag(dagId, schedule))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)

	drs := make([]scheduler.DagRun, dagRuns)
	for i := 0; i < dagRuns; i++ {
		dr := scheduler.DagRun{
			DagId:  dagId,
			AtTime: ts.Add(time.Duration(i) * 23 * 60 * time.Minute),
		}
		drs[i] = dr
	}

	testMaxGoroutineCount(dags, drs, cfg, maxGoroutines, 30*time.Second, nil, t)
}

func TestMaxGoroutineManyParallelTasksLimitedExecutor(t *testing.T) {
	const tasksCount = 50
	const maxGoroutines = 10
	const executorMaxGoroutines = 5
	cfg := scheduler.DefaultConfig
	cfg.TaskSchedulerConfig.MaxGoroutineCount = maxGoroutines
	execCfg := exec.Config{
		HttpRequestTimeout: 30 * time.Second,
		MaxGoroutineCount:  executorMaxGoroutines,
	}

	dags := dag.Registry{}
	startTs := time.Date(2023, 11, 2, 12, 0, 0, 0, time.UTC)
	var schedule schedule.Schedule = schedule.NewFixed(startTs, time.Hour)
	dagId := dag.Id("many_parallel_tasks_dag")
	dags.Add(manyParallelWaitTasksDag(dagId, schedule, tasksCount))
	ts := time.Date(2024, 2, 4, 12, 0, 0, 0, time.UTC)
	dr := scheduler.DagRun{DagId: dagId, AtTime: ts}

	drs := []scheduler.DagRun{dr}
	testMaxGoroutineCount(dags, drs, cfg, maxGoroutines, 30*time.Second,
		&execCfg, t)
}

func testMaxGoroutineCount(
	dags dag.Registry,
	drs []scheduler.DagRun,
	cfg scheduler.Config,
	expectedMaxGoroutines int,
	drTimeout time.Duration,
	executorConfig *exec.Config,
	t *testing.T,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	queues := scheduler.DefaultQueues(cfg)
	notifications := make([]string, 0)

	dbClient, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(dbClient, t)

	sched, _, logsDbClient := schedulerWithSqlite(
		queues, cfg, &notifications, dbClient, nil, t,
	)

	testServer := httptest.NewServer(sched.Start(ctx, dags))
	defer testServer.Close()
	defer cancel()

	// Start executor
	notifier := notify.NewLogsErr(slog.Default())
	go func() {
		executor := exec.New(
			testServer.URL, logsDbClient, nil, nil, nil, notifier,
		)
		executor.Start(dags)
	}()

	var currentMax int64
	doneChan := make(chan struct{})
	go func(currentMax *int64, doneChan <-chan struct{}) {
		for {
			select {
			case <-doneChan:
				return
			default:
				time.Sleep(1 * time.Millisecond)
				gc := sched.Goroutines()
				if gc > *currentMax {
					atomic.StoreInt64(currentMax, gc)
				}
			}
		}
	}(&currentMax, doneChan)

	for _, dr := range drs {
		scheduleNewDagRun(dbClient, queues, dr, t)
	}

	// Wait for DAG run completion or timeout.
	const poll = 10 * time.Millisecond
	for _, dr := range drs {
		waitForDagRunCompletion(dbClient, dr, poll, drTimeout, true, t)
	}
	doneChan <- struct{}{}

	if currentMax <= 1 {
		t.Errorf("Expected at least two goroutines, got %d", currentMax)
	}
	if currentMax > int64(expectedMaxGoroutines) {
		t.Errorf("Expected at most %d goroutines while running Scheduler, got: %d",
			expectedMaxGoroutines, currentMax)
	}
}

func manyParallelWaitTasksDag(
	dagId dag.Id, schedule schedule.Schedule, tasks int,
) dag.Dag {
	start := dag.NewNode(emptyTask{taskId: "start"})
	pTasks := make([]*dag.Node, 0, tasks)

	for i := 0; i < tasks; i++ {
		id := fmt.Sprintf("task_%d", i)
		rand := rand.Intn(200) + 400
		node := dag.NewNode(
			waitTask{
				taskId:   id,
				interval: time.Duration(rand) * time.Millisecond},
		)
		pTasks = append(pTasks, node)
	}
	finish := dag.NewNode(emptyTask{taskId: "finish"})

	start.NextAsyncAndMerge(pTasks, finish)

	d := dag.New(dagId).AddRoot(start).AddSchedule(schedule).Done()
	return d
}
