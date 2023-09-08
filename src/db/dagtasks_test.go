package db

import (
	"fmt"
	"go_shed/src/dag"
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

func TestDagTasksSingleInsertAndReadSimple(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}

	tx, _ := c.dbConn.Begin()
	task := tasks.PrintTask{Name: "db_test"}
	err = c.insertSingleDagTask(tx, "db_dag", task)
	cErr := tx.Commit()
	if cErr != nil {
		t.Error(cErr)
	}
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

func TestDagTestDoubleSingleInsertAndRead(t *testing.T) {
	const DAG_ID = "db_dag"
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}

	// Insert first db_dag.db_test dagtask
	tx, _ := c.dbConn.Begin()
	task := tasks.PrintTask{Name: "db_test"}
	err = c.insertSingleDagTask(tx, "db_dag", task)
	cErr := tx.Commit()
	if cErr != nil {
		t.Error(cErr)
	}
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
	tx, _ = c.dbConn.Begin()
	task2 := tasks.WaitTask{TaskId: "db_test", Interval: 5 * time.Second}
	err = c.insertSingleDagTask(tx, "db_dag", task2)
	cErr = tx.Commit()
	if cErr != nil {
		t.Error(cErr)
	}
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

func TestInsertDagTasks(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
		return
	}

	d := simpleDag("simple_dag", 2)
	iErr := c.InsertDagTasks(d)
	if iErr != nil {
		t.Errorf("Error while inserting simple_dag tasks: %s", iErr.Error())
		return
	}
	rowCnt := c.Count("dagtasks")
	if rowCnt != 3 {
		t.Errorf("Expected 3 rows (3 tasks of simple_dag) after the first insert, got: %d", rowCnt)
	}
	rowCurrentCnt := c.CountWhere("dagtasks", "IsCurrent=1")
	if rowCurrentCnt != 3 {
		t.Errorf("Expected 3 rows with IsCurrent=1 after the first insert, got: %d", rowCurrentCnt)
	}

	time.Sleep(1 * time.Millisecond)

	// Let's modify simple_dag and try to insert once again
	d2 := simpleDag("simple_dag", 9)
	iErr = c.InsertDagTasks(d2)
	if iErr != nil {
		t.Errorf("Error while inserting modified simple_dag tasks: %s", iErr.Error())
		return
	}
	rowCnt = c.Count("dagtasks")
	if rowCnt != 13 {
		t.Errorf("Expected 13 rows (3 from first insert and 10 from another), got: %d", rowCnt)
	}
	rowCurrentCnt = c.CountWhere("dagtasks", "IsCurrent=1")
	if rowCurrentCnt != 10 {
		t.Errorf("Expected 10 rows with IsCurrent=1 after the second insert, got: %d", rowCurrentCnt)
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

	attr := dag.Attr{Id: dag.Id(dagId), Schedule: "5 7 * * *"}
	dag := dag.New(attr, &start)

	return dag
}
