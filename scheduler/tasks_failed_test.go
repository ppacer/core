package scheduler

import (
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
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
