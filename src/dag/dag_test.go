package dag

import (
	"fmt"
	"testing"
	"time"
)

var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)

type EmptyTask struct {
	TaskId string
}

func (et EmptyTask) Id() string { return et.TaskId }
func (et EmptyTask) Execute()   { fmt.Println(et.TaskId); fmt.Println("crap") }

func TestDagNew(t *testing.T) {
	start := Node{Task: EmptyTask{"start"}}
	end := Node{Task: EmptyTask{"end"}}
	start.Next(&end)

	sched := IntervalSchedule{startTs, 5 * time.Second}
	dag := New("mock_dag").AddSchedule(sched).AddRoot(&start).Done()
	fmt.Println(dag)

	if dag.Id != "mock_dag" {
		t.Errorf("Expected Id 'mock_dag', got: %s\n", dag.Id)
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
	// g has two tasks with the same ID
	g := deep3Width3Graph()
	d := New(Id("test_dag")).AddRoot(g).Done()
	if d.IsValid() {
		t.Errorf("Expected dag %s to be invalid (duplicated task IDs), but is valid.", d.String())
	}
}

func TestDagIsValidSimpleLL(t *testing.T) {
	g := linkedList(100)
	d := New(Id("mock_dag")).AddRoot(g).Done()
	if !d.IsValid() {
		t.Errorf("Expected dag %s to be valid, but is not.", d.String())
	}
}

func TestDagIsValidSimpleLLTooLong(t *testing.T) {
	g := linkedList(MAX_RECURSION + 1)
	d := New(Id("mock_dag")).AddRoot(g).Done()
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

	d := New(Id("mock_dag")).AddRoot(&n1).Done()
	if d.IsValid() {
		t.Error("Expected dag to be invalid (cylic), but is valid.")
	}
}

func TestDagIsValidSimpleNonuniqueIds(t *testing.T) {
	n1 := Node{Task: nameTask{Name: "n1"}}
	n2 := Node{Task: nameTask{Name: "n2"}}
	n3 := Node{Task: nameTask{Name: "n1"}}
	n1.Next(&n2)
	n2.Next(&n3)

	d := New(Id("mock_dag")).AddRoot(&n1).Done()
	if d.IsValid() {
		t.Error("Expected dag to be invalid (have non unique task IDs), but is valid.")
	}
}

func TestDagGetTaskSimple(t *testing.T) {
	g := linkedList(100)
	d := New(Id("mock_dag")).AddRoot(g).Done()
	t50, err := d.GetTask("step_50")
	if err != nil {
		t.Errorf("Expected nil error while GetTask, but got: %s", err.Error())
	}
	if t50.Id() != "step_50" {
		t.Errorf("Expected task Id 'step_50', got: %s", t50.Id())
	}
}

func TestDagGetTaskInvalidId(t *testing.T) {
	g := linkedList(100)
	d := New(Id("mock_dag")).AddRoot(g).Done()
	_, err := d.GetTask("step_500")
	if err == nil {
		t.Error("Expected non-nil error while getting 'step_500', but got empty error.")
	}
	if err != ErrTaskNotFoundInDag {
		t.Errorf("Expected non-nill ErrTaskNotFoundInDag, but got: %v", err)
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

	dag := New(Id("mock_dag_2")).AddSchedule(IntervalSchedule{startTs, 1 * time.Hour}).AddRoot(&start).Done()
	fmt.Println(dag)
}
