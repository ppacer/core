package scheduler

import (
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
)

func TestSchouldBeRetried(t *testing.T) {
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
		// TODO: more nodes
	}

	// Expand DAG before
	for _, node := range nodes {
		tail.Next(node)
		tail = node
	}

	testData := []struct {
		drt             DagRunTask
		status          dag.TaskStatus
		shouldBeRetried bool
	}{
		{newDrt("start", 0), dag.TaskSuccess, false},
		{newDrt("t1", 0), dag.TaskRunning, false},
		{newDrt("t2", 0), dag.TaskFailed, true},
		{newDrt("t2", 0), dag.TaskSuccess, false},
		{newDrt("t2", 5), dag.TaskFailed, true},
		{newDrt("t2", 5), dag.TaskRunning, false},
		{newDrt("t2", 5), dag.TaskSuccess, false},
		{newDrt("t2", 10), dag.TaskFailed, false},
		{newDrt("t2", 10), dag.TaskScheduled, false},
		{newDrt("t2", 10), dag.TaskRunning, false},
		// TODO: more cases!
	}

	for _, data := range testData {
		shouldBe, err := ftm.ShouldBeRetried(data.drt, data.status)
		if err != nil {
			t.Errorf("Error while ShouldBeRetried for %+v (%s): %s",
				data.drt, data.status.String(), err.Error())
		}
		if shouldBe != data.shouldBeRetried {
			t.Errorf("Expected shouldBeRetried=%v, but got %v",
				data.shouldBeRetried, shouldBe)
		}
	}
}

func newFailedTaskManagerForTests(t *testing.T) *failedTaskManager {
	dbClient, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	dags := dag.Registry{}
	taskQueue := ds.NewSimpleQueue[DagRunTask](100)
	taskCache := ds.NewLruCache[DRTBase, DagRunTaskState](100)
	return newFailedTaskManager(
		dags, dbClient, &taskQueue, taskCache, DefaultTaskSchedulerConfig,
		simpleLogger(),
	)
}
