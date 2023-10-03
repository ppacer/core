package db

import (
	"context"
	"database/sql"
	"fmt"
	"go_shed/src/dag"
	"go_shed/src/timeutils"
	"go_shed/src/user/tasks"
	"math/rand"
	"testing"
	"time"
)

var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)

func TestDagTestReadFromEmptyTable(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	_, rErr := c.ReadDagTask(ctx, "hello", "say_hello")
	if rErr == nil {
		t.Error("Expected ReadDagTask fail to reading non existent task")
	}
	if rErr != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, but got: %s", rErr.Error())
	}
}

func TestDagTasksSingleInsertAndReadSimple(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	tx, _ := c.dbConn.Begin()
	task := tasks.PrintTask{Name: "db_test"}
	insertTs := timeutils.ToString(time.Now())
	err = c.insertSingleDagTask(ctx, tx, "db_dag", task, insertTs)
	cErr := tx.Commit()
	if cErr != nil {
		t.Error(cErr)
	}
	if err != nil {
		t.Errorf("Unexpected error while trying inserting dagtask to in-memory DB: %s", err.Error())
	}

	readTask, err := c.ReadDagTask(ctx, "db_dag", task.Id())
	if err != nil {
		t.Errorf("Error while reading just inserted dagtask from in-memory DB: %s", err.Error())
	}
	if readTask.DagId != "db_dag" {
		t.Errorf("Expected DagId db_dag, got: %s", readTask.DagId)
	}
	if readTask.TaskId != task.Id() {
		t.Errorf("Expected TaskId db_test, got: %s", readTask.TaskId)
	}
	if readTask.TaskTypeName != "PrintTask" {
		t.Errorf("Expected TaskTypeName PrintTask, got: %s", readTask.TaskTypeName)
	}
}

func TestInsertDagTasks(t *testing.T) {
	const maxTasks = 25
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	dagId := "simple_dag"
	innerTasks1 := rand.Intn(maxTasks) + 1
	d := simpleDag(dagId, innerTasks1)
	dTaskNum := len(d.Flatten())
	iErr := c.InsertDagTasks(ctx, d)
	if iErr != nil {
		t.Errorf("Error while inserting simple_dag tasks: %s", iErr.Error())
		return
	}
	rowCnt := c.Count("dagtasks")
	if rowCnt != dTaskNum {
		t.Errorf("Expected %d rows (%d tasks of simple_dag) after the first insert, got: %d",
			dTaskNum, dTaskNum, rowCnt)
		logDagTasks(c, dagId, t)
	}
	rowCurrentCnt := c.CountWhere("dagtasks", "IsCurrent=1")
	if rowCurrentCnt != dTaskNum {
		t.Errorf("Expected %d rows with IsCurrent=1 after the first insert, got: %d", dTaskNum, rowCurrentCnt)
		logDagTasks(c, dagId, t)
	}

	time.Sleep(1 * time.Millisecond)

	// Let's modify simple_dag and try to insert once again
	innerTasks2 := rand.Intn(maxTasks) + 1
	d2 := simpleDag("simple_dag", innerTasks2)
	dTaskNum2 := len(d2.Flatten())
	iErr = c.InsertDagTasks(ctx, d2)
	if iErr != nil {
		t.Errorf("Error while inserting modified simple_dag tasks: %s", iErr.Error())
		return
	}
	rowCnt = c.Count("dagtasks")
	if rowCnt != dTaskNum+dTaskNum2 {
		t.Errorf("Expected %d rows (%d from first insert and %d from another), got: %d",
			dTaskNum+dTaskNum2, dTaskNum, dTaskNum2, rowCnt)
		logDagTasks(c, dagId, t)
	}
	rowCurrentCnt = c.CountWhere("dagtasks", "IsCurrent=1")
	if rowCurrentCnt != dTaskNum2 {
		t.Errorf("Expected %d rows with IsCurrent=1 after the second insert, got: %d", dTaskNum2, rowCurrentCnt)
		logDagTasks(c, dagId, t)
	}
}

func TestInsertEmptyDagTasks(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}

	// DAG with no tasks
	ctx := context.Background()
	sched := dag.FixedSchedule{Start: startTs, Interval: 1 * time.Hour}
	d := dag.New(dag.Id("test")).AddSchedule(sched).Done()

	iErr := c.InsertDagTasks(ctx, d)
	if iErr != nil {
		t.Errorf("Error while inserting simple_dag tasks: %s", iErr.Error())
		return
	}
	rowCnt := c.Count("dagtasks")
	if rowCnt != 0 {
		t.Errorf("Expected no rows for dagtasks in this case, got: %d", rowCnt)
		logDagTasks(c, "test", t)
	}
	rowCnt = c.CountWhere("dagtasks", "DagId='test'")
	if rowCnt != 0 {
		t.Errorf("Expected no rows for dagtasks where DagId=test, got: %d", rowCnt)
		logDagTasks(c, "test", t)
	}
}

func BenchmarkDagTasksInsert(b *testing.B) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		b.Error(err)
		return
	}
	ctx := context.Background()
	d := simpleDag("simple_dag", 99)

	for i := 0; i < b.N; i++ {
		iErr := c.InsertDagTasks(ctx, d)
		if iErr != nil {
			b.Errorf("Error while inserting simple_dag tasks: %s", iErr.Error())
			return
		}
	}
}

func simpleDag(dagId string, innerTasks int) dag.Dag {
	start := dag.Node{Task: tasks.WaitTask{TaskId: "start", Interval: 3 * time.Second}}
	prev := &start

	for i := 0; i < innerTasks; i++ {
		t := dag.Node{Task: tasks.PrintTask{Name: fmt.Sprintf("t%d", i)}}
		prev.Next(&t)
		prev = &t
	}

	sched := dag.FixedSchedule{Start: startTs, Interval: 1 * time.Hour}
	dag := dag.New(dag.Id(dagId)).AddSchedule(sched).AddRoot(&start).Done()

	return dag
}

func logDagTasks(c *Client, dagId string, t *testing.T) {
	ctx := context.Background()
	dts, err := c.ReadDagTasks(ctx, dagId)
	if err != nil {
		t.Errorf("Could not read dagtasks for dagId=%s for debugging", dagId)
	}
	t.Logf("dagtasks row for dagId=%s:\n", dagId)
	for _, dt := range dts {
		isCurrInt := 0
		if dt.IsCurrent {
			isCurrInt = 1
		}
		fmt.Printf("%s|%s|%d|%s|%s\n",
			dt.DagId, dt.TaskId, isCurrInt, dt.InsertTs, dt.TaskTypeName)
	}
}
