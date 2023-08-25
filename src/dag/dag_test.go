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

func TestDagNew(t *testing.T) {
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

func TestDagIsValidSimple(t *testing.T) {
	attr := Attr{Id: "mock_dag", Schedule: ""}
	// g has two tasks with the same ID
	g := deep3Width3Graph()
	d := New(attr, g)
	if d.IsValid() {
		t.Errorf("Expected dag %s to be invalid (duplicated task IDs), but is valid.", d.String())
	}
}

func TestDagIsValidSimpleLL(t *testing.T) {
	attr := Attr{Id: "mock_dag", Schedule: ""}
	g := linkedList(100)
	d := New(attr, g)
	if !d.IsValid() {
		t.Errorf("Expected dag %s to be valid, but is not.", d.String())
	}
}

func TestDagIsValidSimpleLLTooLong(t *testing.T) {
	attr := Attr{Id: "mock_dag", Schedule: ""}
	g := linkedList(MAX_RECURSION + 1)
	d := New(attr, g)
	if d.IsValid() {
		t.Error("Expected dag to be invalid (too deep), but is valid.")
	}
}

func TestDagIsValidSimpleCyclic(t *testing.T) {
	n1 := Node{Task: nameTask{Name: "n1"}}
	n2 := Node{Task: nameTask{Name: "n2"}}
	n3 := Node{Task: nameTask{Name: "n3"}}
	n1.Next(&n2)
	n2.Next(&n3)
	n3.Next(&n1)

	attr := Attr{Id: "mock_dag", Schedule: ""}
	d := New(attr, &n1)
	if d.IsValid() {
		t.Error("Expected dag to be invalid (cylic), but is valid.")
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
