package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dskrzypiec/scheduler/src/timeutils"
)

func TestInsertDagRunTaskSimple(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag"
	execTs := timeutils.ToString(time.Now())
	taskId := "my_task_1"
	iErr := c.InsertDagRunTask(ctx, dagId, execTs, taskId)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}

	c1 := c.Count("dagruntasks")
	if c1 != 1 {
		t.Errorf("Expected 1 row got: %d", c1)
	}

	taskId2 := "my_task_2"
	iErr2 := c.InsertDagRunTask(ctx, dagId, execTs, taskId2)
	if iErr2 != nil {
		t.Errorf("Error while inserting dag run: %s", iErr2.Error())
	}

	c2 := c.Count("dagruntasks")
	if c2 != 2 {
		t.Errorf("Expected 2 row got: %d", c2)
	}

}

func TestReadDagRunTasksFromEmpty(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	drts, err := c.ReadDagRunTasks(ctx, "any_dag", "any_time")
	if err != nil {
		t.Errorf("Expected non-nil error, got: %s", err.Error())
	}
	if len(drts) != 0 {
		t.Errorf("Expected 0 loaded DagRunTasks, got: %d", len(drts))
	}
}

func TestReadDagRunTasks(t *testing.T) {
	const N = 100
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag"
	execTs := timeutils.ToString(time.Now())

	for i := 0; i < N; i++ {
		taskId := fmt.Sprintf("my_task_%d", i)
		insertDagRunTask(c, ctx, dagId, execTs, taskId, t)
	}

	dagTasks, rErr := c.ReadDagRunTasks(ctx, dagId, execTs)
	if rErr != nil {
		t.Errorf("Unexpected error while reading dag run tasks: %s",
			rErr.Error())
	}
	if len(dagTasks) != N {
		t.Errorf("Expected %d dag run tasks, got: %d", N, len(dagTasks))
	}

	for _, dagTask := range dagTasks {
		if dagTask.ExecTs != execTs {
			t.Errorf("Expected ExecTs=%s, got: %s", execTs, dagTask.ExecTs)
		}
		if dagTask.Status != DagRunTaskStatusScheduled {
			t.Errorf("Expeted status=%s, got: %s", DagRunTaskStatusScheduled,
				dagTask.Status)
		}
	}
}

func insertDagRunTask(
	c *Client,
	ctx context.Context,
	dagId, execTs, taskId string,
	t *testing.T,
) {
	iErr := c.InsertDagRunTask(ctx, dagId, execTs, taskId)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
}
