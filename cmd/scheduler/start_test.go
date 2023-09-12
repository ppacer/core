package main

import (
	"go_shed/src/dag"
	"go_shed/src/db"
	"go_shed/src/user/tasks"
	"path"
	"testing"
	"time"
)

var sqlSchemaPath = path.Join("..", "..", "schema.sql")

func TestSyncOneDagNoChanges(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(c, d)
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s", rErr.Error())
	}
	dagtasksDb1, dtErr := c.ReadDagTasks(dagId)
	if dtErr != nil {
		t.Fatalf("Unexpected error while reading dag tasks from dagtasks table: %s", dtErr.Error())
	}
	if len(dagtasksDb1) != tasksNum {
		t.Fatalf("Expected %d dag tasks in dagtasks table, got: %d", tasksNum, len(dagtasksDb1))
	}

	// Second sync - should not change anything
	s2Err := syncDag(c, d)
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb2, r2Err := c.ReadDag(dagId)
	if r2Err != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s", r2Err.Error())
	}

	dagtasksDb2, dt2Err := c.ReadDagTasks(dagId)
	if dt2Err != nil {
		t.Fatalf("Unexpected error while reading dag tasks from dagtasks table: %s", dt2Err.Error())
	}
	if len(dagtasksDb2) != tasksNum {
		t.Fatalf("Expected %d dag tasks in dagtasks table, got: %d", tasksNum, len(dagtasksDb1))
	}

	if dagDb1 != dagDb2 {
		t.Fatalf("Expected the same dag row from dags table after the second sync. After 1st: [%v], after 2nd: [%v]",
			dagDb1, dagDb2)
	}

	if len(dagtasksDb1) != len(dagtasksDb2) {
		t.Fatalf("Number of dagtasks changed after the second sync from %d, to %d",
			len(dagtasksDb1), len(dagtasksDb2))
	}

	for i := 0; i < len(dagtasksDb1); i++ {
		if dagtasksDb1[i] != dagtasksDb2[i] {
			t.Fatalf("Expected unchanged dagtasks after the second sync, got diff for i=%d: from [%v] to [%v]",
				i, dagtasksDb1[i], dagtasksDb2[i])
		}
	}
}

func TestSyncOneDagChangingAttr(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(c, d)
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s", rErr.Error())
	}

	// Update DAG attributes
	d.Attr.Schedule = "8 8 * * 1-5"

	// Second sync - should not change anything
	s2Err := syncDag(c, d)
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb2, r2Err := c.ReadDag(dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag after the update from dags table: %s", r2Err.Error())
	}
	if dagDb1 == dagDb2 {
		t.Fatal("Expected the row in dags table to be different after DAG's attr were updated")
	}
	if dagDb1.HashAttributes == dagDb2.HashAttributes {
		t.Errorf("Expected different HashAttr after the updated, but is unchanged: %s", dagDb1.HashAttributes)
	}
	if dagDb1.Attributes == dagDb2.Attributes {
		t.Errorf("Expected different Attributes after the updated, but is unchanged: %s", dagDb1.Attributes)
	}
	if dagDb1.HashTasks != dagDb2.HashTasks {
		t.Errorf("Expected HashTasks to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.HashTasks, dagDb1.HashTasks)
	}
}

func TestSyncOneDagChangingTasks(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(c, d)
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s", rErr.Error())
	}

	// Update DAG tasks
	additionalTask := dag.Node{Task: tasks.PrintTask{Name: "bonus_task"}}
	d.Root.Next(&additionalTask)

	// Second sync - should not change anything
	s2Err := syncDag(c, d)
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum+tasksNum+1, c, t)

	currentDagTasks := c.CountWhere("dagtasks", "IsCurrent=1")
	if currentDagTasks != tasksNum+1 {
		t.Fatalf("Expected %d current rows in dagtasks table after the update, got: %d", tasksNum+1, currentDagTasks)
	}

	dagDb2, r2Err := c.ReadDag(dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag after the update from dags table: %s", r2Err.Error())
	}
	if dagDb1 == dagDb2 {
		t.Fatal("Expected the row in dags table to be different after DAG's attr were updated")
	}
	if dagDb1.HashTasks == dagDb2.HashTasks {
		t.Errorf("Expected different HashTasks after the update, but is unchanged: %s", dagDb1.HashTasks)
	}
	if dagDb1.HashAttributes != dagDb2.HashAttributes {
		t.Errorf("Expected HashAttr to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.HashAttributes, dagDb2.HashAttributes)
	}
	if dagDb1.Attributes != dagDb2.Attributes {
		t.Errorf("Expected Attributes to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.Attributes, dagDb2.Attributes)
	}
}

func dagtasksCountCheck(expDagCnt, expDagTasksCnt int, c *db.Client, t *testing.T) {
	dagCount := c.Count("dags")
	if dagCount != expDagCnt {
		t.Fatalf("Expected %d row in dags table, got: %d", expDagCnt, dagCount)
	}
	dagtasksCount := c.Count("dagtasks")
	if dagtasksCount != expDagTasksCnt {
		t.Fatalf("Expected %d rows in dagtasks table, got: %d", expDagTasksCnt, dagtasksCount)
	}
}

func verySimpleDag(dagId string) dag.Dag {
	start := dag.Node{Task: tasks.WaitTask{TaskId: "start", Interval: 3 * time.Second}}
	t := dag.Node{Task: tasks.PrintTask{Name: "t_1"}}
	end := dag.Node{Task: tasks.WaitTask{TaskId: "end", Interval: 1 * time.Second}}
	start.Next(&t)
	t.Next(&end)

	attr := dag.Attr{Id: dag.Id(dagId), Schedule: "5 7 * * *"}
	dag := dag.New(attr, &start)
	return dag
}
