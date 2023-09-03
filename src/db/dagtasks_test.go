package db

import (
	"go_shed/src/user/tasks"
	"testing"
	"time"
)

func TestDagTestReadFromEmptyTable(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}
	_, rErr := c.ReadDagTask("hello", "say_hello")
	if rErr == nil {
		t.Error("Expected ReadDagTask fail to reading non existent task")
	}
}

func TestDagTasksInsertAndReadSimple(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}

	task := tasks.PrintTask{Name: "db_test"}
	err = c.InsertDagTask("db_dag", task)
	if err != nil {
		t.Errorf("Unexpected error while trying inserting dagtask to in-memory DB: %s", err.Error())
	}

	readTask, err := c.ReadDagTask("db_dag", task.Id())
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

func TestDagTestDoubleInsertAndRead(t *testing.T) {
	const DAG_ID = "db_dag"
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}

	// Insert first db_dag.db_test dagtask
	task := tasks.PrintTask{Name: "db_test"}
	err = c.InsertDagTask(DAG_ID, task)
	if err != nil {
		t.Errorf("Unexpected error while trying inserting dbNameTask to in-memory DB: %s", err.Error())
	}

	// Checks
	tdb, rErr := c.ReadDagTask(DAG_ID, task.Id())
	if rErr != nil {
		t.Errorf("Error while reading DagTask from DB: %s", rErr.Error())
	}
	if tdb.DagId != DAG_ID {
		t.Errorf("Expected DagId db_dag, got: %s", tdb.DagId)
	}
	if tdb.TaskId != task.Id() {
		t.Errorf("Expected TaskId db_test, got: %s", tdb.TaskId)
	}
	if tdb.TaskTypeName != "PrintTask" {
		t.Errorf("Expected TaskTypeName PrintTask, got: %s", tdb.TaskTypeName)
	}
	printTaskSource := `{
	fmt.Println("Hello executor!")
}`
	if tdb.TaskBodySource != printTaskSource {
		t.Errorf("Expected task body source [%s], got [%s]", printTaskSource, tdb.TaskBodySource)
	}

	// Need at least 1ms difference in time to produce distinguish insertTs
	time.Sleep(1 * time.Millisecond)

	// Second insert for db_dag.db_test
	task2 := tasks.WaitTask{TaskId: "db_test", Interval: 5 * time.Second}
	err = c.InsertDagTask(DAG_ID, task2)
	if err != nil {
		t.Errorf("Unexpected error while trying inserting second dbNameTask to in-memory DB: %s", err.Error())
		return
	}

	// Checks for the second row (the current one)
	t2db, r2Err := c.ReadDagTask(DAG_ID, task.Id())
	if r2Err != nil {
		t.Errorf("Error while second reading DagTask from DB: %s", r2Err.Error())
		return
	}
	if t2db.DagId != DAG_ID {
		t.Errorf("Expected DagId db_dag, got: %s", t2db.DagId)
	}
	if t2db.TaskId != task.Id() {
		t.Errorf("Expected TaskId db_test, got: %s", t2db.TaskId)
	}
	if !t2db.IsCurrent {
		t.Error("Expected second inserted row to be the current one")
	}
	if t2db.TaskTypeName != "WaitTask" {
		t.Errorf("Expected TaskTypeName WaitTask, got: %s", t2db.TaskTypeName)
	}
	waitTaskSource := `{
	log.Info().Msgf("Task [%s] starts sleeping for %v...", wt.Id(), wt.Interval)
	time.Sleep(wt.Interval)
	log.Info().Msgf("Task [%s] is done", wt.Id())
}`
	if t2db.TaskBodySource != waitTaskSource {
		t.Errorf("Expected task body source [%s], got [%s]", waitTaskSource, t2db.TaskBodySource)
	}
}
