package dag

import (
	"fmt"
	"testing"
)

type EmptyTask struct {
	TaskId string
}

func (et EmptyTask) Id() string { return et.TaskId }
func (et EmptyTask) Execute()   { fmt.Println(et.TaskId); fmt.Println("crap") }

func TestDag(t *testing.T) {
	start := Node{Task: EmptyTask{"start"}}
	end := Node{Task: EmptyTask{"end"}}
	start.Next(&end)

	dagAttr := Attr{Id: "mock_dag", Schedule: "5 * * * *"}
	dag := New(dagAttr, &start)
	fmt.Println(dag)

	if dag.Attr.Id != "mock_dag" {
		t.Errorf("Expected Id 'mock_dag', got: %s\n", dag.Attr.Id)
	}
	if dag.Root.Task.Id() != "start" {
		t.Errorf("Expected first task Id 'start', got: %s\n", dag.Root.Task.Id())
	}
	if len(dag.Root.Children) != 1 {
		t.Errorf("Expected single attached task to 'start', got: %d\n", len(dag.Root.Children))
	}
	secondTask := dag.Root.Children[0]
	if secondTask.Task.Id() != "end" {
		t.Errorf("Expected second task 'end', got: %s\n", secondTask.Task.Id())
	}
}

func TestDagPrint(t *testing.T) {
	start := Node{Task: EmptyTask{"start"}}
	t1 := Node{Task: EmptyTask{"t1"}}
	t2 := Node{Task: EmptyTask{"t2"}}
	t3 := Node{Task: EmptyTask{"t3"}}
	t4 := Node{Task: EmptyTask{"t4"}}
	end := Node{Task: EmptyTask{"end"}}

	start.Next(&t1)
	start.Next(&t2)
	t1.Next(&t3)
	t2.Next(&t4)
	t3.Next(&t4)
	t4.Next(&end)

	attr := Attr{Id: "mock_dag_2", Schedule: "5 7 * * *"}
	dag := New(attr, &start)
	fmt.Println(dag)
}
