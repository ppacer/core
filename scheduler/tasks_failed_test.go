package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/timeutils"
)

func TestShouldBeRetried(t *testing.T) {
	const dagId = "linear_dag"
	execTs := time.Now()
	ftm := newFailedTaskManagerForTests(t)
	defer db.CleanUpSqliteTmp(ftm.dbClient, t)

	et := func(taskId string) EmptyTask {
		return EmptyTask{TaskId: taskId}
	}

	newDrt := func(taskId string, retry int) DagRunTask {
		return DagRunTask{
			DagId:  dagId,
			AtTime: execTs,
			TaskId: taskId,
			Retry:  retry,
		}
	}

	root := dag.NewNode(EmptyTask{TaskId: "start"})
	tail := root
	linearDag := dag.New(dag.Id(dagId)).AddRoot(root).Done()
	ftm.dags.Add(linearDag)

	nodes := []*dag.Node{
		dag.NewNode(et("t1")),
		dag.NewNode(et("t2"), dag.WithTaskRetries(10)),
		dag.NewNode(
			et("t3"),
			dag.WithTaskRetries(3),
			dag.WithTaskRetriesDelay(30*time.Second),
		),
		dag.NewNode(
			et("t4"),
			dag.WithTaskRetries(2),
			dag.WithTaskRetriesDelay(150*time.Millisecond),
		),
		// TODO: more nodes
	}

	// Expand DAG before
	for _, node := range nodes {
		tail.Next(node)
		tail = node
	}

	testData := []struct {
		drt              DagRunTask
		status           dag.TaskStatus
		shouldBeRetried  bool
		delayBeforeRetry float64
	}{
		{newDrt("start", 0), dag.TaskSuccess, false, 0},
		{newDrt("t1", 0), dag.TaskRunning, false, 0},
		{newDrt("t2", 0), dag.TaskFailed, true, 0},
		{newDrt("t2", 0), dag.TaskSuccess, false, 0},
		{newDrt("t2", 5), dag.TaskFailed, true, 0},
		{newDrt("t2", 5), dag.TaskRunning, false, 0},
		{newDrt("t2", 5), dag.TaskSuccess, false, 0},
		{newDrt("t2", 10), dag.TaskFailed, false, 0},
		{newDrt("t2", 10), dag.TaskScheduled, false, 0},
		{newDrt("t2", 10), dag.TaskRunning, false, 0},
		{newDrt("t3", 0), dag.TaskFailed, true, 30.0},
		{newDrt("t4", 0), dag.TaskFailed, true, 0.150},
	}

	for _, data := range testData {
		shouldBe, delay, err := ftm.ShouldBeRetried(data.drt, data.status)
		if err != nil {
			t.Errorf("Error while ShouldBeRetried for %+v (%s): %s",
				data.drt, data.status.String(), err.Error())
		}
		if shouldBe != data.shouldBeRetried {
			t.Errorf("Expected shouldBeRetried=%v, but got %v",
				data.shouldBeRetried, shouldBe)
		}
		if delay.Seconds() != data.delayBeforeRetry {
			t.Errorf("Expected delayBeforeRetry=%v, got: %v",
				data.delayBeforeRetry, delay)
		}
	}
}

func TestShouldBeRetriedDagRunRestarts(t *testing.T) {
	ctx := context.Background()
	const dagId = "linear_dag"
	ftm := newFailedTaskManagerForTests(t)
	defer db.CleanUpSqliteTmp(ftm.dbClient, t)

	et := func(taskId string) EmptyTask {
		return EmptyTask{TaskId: taskId}
	}

	newDrt := func(taskId string, execTs time.Time, retry int) DagRunTask {
		return DagRunTask{
			DagId:  dagId,
			AtTime: execTs,
			TaskId: taskId,
			Retry:  retry,
		}
	}

	root := dag.NewNode(EmptyTask{TaskId: "start"})
	tail := root
	linearDag := dag.New(dag.Id(dagId)).AddRoot(root).Done()
	ftm.dags.Add(linearDag)

	nodes := []*dag.Node{
		dag.NewNode(et("t1")),
		dag.NewNode(et("t2"), dag.WithTaskRetries(10)),
		dag.NewNode(
			et("t3"),
			dag.WithTaskRetries(3),
			dag.WithTaskRetriesDelay(30*time.Second),
		),
		dag.NewNode(
			et("t4"),
			dag.WithTaskRetries(2),
			dag.WithTaskRetriesDelay(150*time.Millisecond),
		),
	}

	// Expand DAG before
	for _, node := range nodes {
		tail.Next(node)
		tail = node
	}

	execTsNoRes := time.Now()
	execTs1Res := execTsNoRes.Add(1 * time.Hour)
	const N = 3
	execTs3Res := execTsNoRes.Add(32 * time.Hour)

	// Insert whole DAG runs restarts info
	iErr1 := ftm.dbClient.InsertDagRunNextRestart(
		ctx, dagId, timeutils.ToString(execTs1Res),
	)
	if iErr1 != nil {
		t.Fatalf("Cannot insert DAG run restart: %s", iErr1.Error())
	}
	for i := 0; i < N; i++ {
		iErr2 := ftm.dbClient.InsertDagRunNextRestart(
			ctx, dagId, timeutils.ToString(execTs3Res),
		)
		if iErr2 != nil {
			t.Fatalf("Cannot insert DAG run restart: %s", iErr2.Error())
		}
	}

	testData := []struct {
		drt              DagRunTask
		status           dag.TaskStatus
		shouldBeRetried  bool
		delayBeforeRetry float64
	}{
		// DAG run with no restarts
		{newDrt("start", execTsNoRes, 0), dag.TaskSuccess, false, 0},
		{newDrt("t1", execTsNoRes, 0), dag.TaskRunning, false, 0},
		{newDrt("t2", execTsNoRes, 0), dag.TaskFailed, true, 0},
		{newDrt("t2", execTsNoRes, 0), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTsNoRes, 5), dag.TaskFailed, true, 0},
		{newDrt("t2", execTsNoRes, 5), dag.TaskRunning, false, 0},
		{newDrt("t2", execTsNoRes, 5), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTsNoRes, 10), dag.TaskFailed, false, 0},
		{newDrt("t2", execTsNoRes, 10), dag.TaskScheduled, false, 0},
		{newDrt("t2", execTsNoRes, 10), dag.TaskRunning, false, 0},
		{newDrt("t3", execTsNoRes, 0), dag.TaskFailed, true, 30.0},
		{newDrt("t4", execTsNoRes, 0), dag.TaskFailed, true, 0.150},

		// DAG run with 1 restart
		{newDrt("start", execTs1Res, 0), dag.TaskSuccess, false, 0},
		{newDrt("t1", execTs1Res, 0), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs1Res, 0), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs1Res, 0), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTs1Res, 5), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs1Res, 5), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs1Res, 5), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTs1Res, 10), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs1Res, 10), dag.TaskScheduled, false, 0},
		{newDrt("t2", execTs1Res, 10), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs1Res, 18), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs1Res, 21), dag.TaskFailed, false, 0},
		{newDrt("t3", execTs1Res, 0), dag.TaskFailed, true, 30.0},
		{newDrt("t4", execTs1Res, 0), dag.TaskFailed, true, 0.150},
		{newDrt("t3", execTs1Res, 3), dag.TaskFailed, true, 30.0},
		{newDrt("t4", execTs1Res, 2), dag.TaskFailed, true, 0.150},
		{newDrt("t3", execTs1Res, 6), dag.TaskFailed, false, 30.0},
		{newDrt("t4", execTs1Res, 5), dag.TaskFailed, false, 0.150},

		// DAG run with 3 restarts
		{newDrt("start", execTs3Res, 0), dag.TaskSuccess, false, 0},
		{newDrt("t1", execTs3Res, 0), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs3Res, 0), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs3Res, 0), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTs3Res, 5), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs3Res, 5), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs3Res, 5), dag.TaskSuccess, false, 0},
		{newDrt("t2", execTs3Res, 10), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs3Res, 10), dag.TaskScheduled, false, 0},
		{newDrt("t2", execTs3Res, 10), dag.TaskRunning, false, 0},
		{newDrt("t2", execTs3Res, 18), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs3Res, 37), dag.TaskFailed, true, 0},
		{newDrt("t2", execTs3Res, 40), dag.TaskFailed, false, 0},
		{newDrt("t3", execTs3Res, 0), dag.TaskFailed, true, 30.0},
		{newDrt("t4", execTs3Res, 0), dag.TaskFailed, true, 0.150},
		{newDrt("t3", execTs3Res, 3), dag.TaskFailed, true, 30.0},
		{newDrt("t3", execTs3Res, 6), dag.TaskFailed, true, 30.0},
		{newDrt("t3", execTs3Res, 12), dag.TaskFailed, false, 30.0},
		{newDrt("t4", execTs3Res, 2), dag.TaskFailed, true, 0.150},
		{newDrt("t4", execTs3Res, 5), dag.TaskFailed, true, 0.150},
		{newDrt("t4", execTs3Res, 8), dag.TaskFailed, false, 0.150},
	}

	for _, data := range testData {
		shouldBe, delay, err := ftm.ShouldBeRetried(data.drt, data.status)
		if err != nil {
			t.Errorf("Error while ShouldBeRetried for %+v (%s): %s",
				data.drt, data.status.String(), err.Error())
		}
		if shouldBe != data.shouldBeRetried {
			t.Errorf("Expected shouldBeRetried=%v, but got %v for %+v (%s)",
				data.shouldBeRetried, shouldBe, data.drt, data.status.String())
		}
		if delay.Seconds() != data.delayBeforeRetry {
			t.Errorf("Expected delayBeforeRetry=%v, got: %v",
				data.delayBeforeRetry, delay)
		}
	}
}

func newFailedTaskManagerForTests(t *testing.T) *failedTaskManager {
	dbClient, err := db.NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	dags := dag.Registry{}
	taskQueue := ds.NewSimpleQueue[DagRunTask](100)
	taskCache := ds.NewLruCache[DRTBase, DagRunTaskState](100)
	return newFailedTaskManager(
		dags, dbClient, &taskQueue, taskCache, DefaultTaskSchedulerConfig,
		testLogger(),
	)
}
