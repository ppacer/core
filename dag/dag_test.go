// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package dag

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

var startTs = time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)

type EmptyTask struct {
	TaskId string
}

func (et EmptyTask) Id() string { return et.TaskId }

func (et EmptyTask) Execute(_ TaskContext) error {
	fmt.Println(et.TaskId)
	fmt.Println("crap")
	return nil
}

func TestDagNew(t *testing.T) {
	start := NewNode(EmptyTask{"start"})
	end := NewNode(EmptyTask{"end"})
	start.Next(end)

	sched := FixedSchedule{startTs, 5 * time.Second}
	dag := New("mock_dag").AddSchedule(sched).AddRoot(start).Done()
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
	n1 := NewNode(nameTask{Name: "n1"})
	n2 := NewNode(nameTask{Name: "n2"})
	n3 := NewNode(nameTask{Name: "n3"})
	n1.Next(n2).Next(n3).Next(n1)

	d := New(Id("mock_dag")).AddRoot(n1).Done()
	if d.IsValid() {
		t.Error("Expected dag to be invalid (cylic), but is valid.")
	}
}

func TestDagIsValidSimpleNonuniqueIds(t *testing.T) {
	n1 := NewNode(nameTask{Name: "n1"})
	n2 := NewNode(nameTask{Name: "n2"})
	n3 := NewNode(nameTask{Name: "n1"})
	n1.Next(n2).Next(n3)

	d := New(Id("mock_dag")).AddRoot(n1).Done()
	if d.IsValid() {
		t.Error("Expected dag to be invalid (have non unique task IDs), but is valid.")
	}
}

func TestDagTaskParentsSimple(t *testing.T) {
	g := fewBranchoutsAndMergesGraph()
	d := New(Id("test_dag")).AddRoot(g).Done()
	parents := d.TaskParents()

	if len(parents) != len(d.Flatten()) {
		t.Errorf("Expected %d tasks in map, got: %d", len(d.Flatten()),
			len(parents))
	}

	expectedParentsNumber := map[string]int{
		"n1":      0,
		"g1n1":    1,
		"g1n21":   1,
		"g1n22":   1,
		"g1n3":    2,
		"g3":      1,
		"finish":  3,
		"g2n1":    1,
		"g2n21":   1,
		"g2n22":   1,
		"g2n23":   1,
		"g2n31":   2,
		"g2n32":   1,
		"g2Merge": 2,
	}

	for taskId, expParentsNum := range expectedParentsNumber {
		par, exists := parents[taskId]
		if !exists {
			t.Errorf("Task %s does not exist in parents map", taskId)
		}
		if len(par) != expParentsNum {
			t.Errorf("Expected %d parents for %s, got %d (%v)", expParentsNum,
				taskId, len(par), par)
		}
	}

	expectedFinishParents := []string{"g1n3", "g3", "g2Merge"}
	finishPar := parents["finish"]
	sort.Strings(expectedFinishParents)
	sort.Strings(finishPar)
	for idx, taskId := range expectedFinishParents {
		if taskId != finishPar[idx] {
			t.Errorf("Expected parent[%d] %s, got %s", idx, taskId,
				finishPar[idx])
		}
	}
}

func TestDagTaskParentsEmpty(t *testing.T) {
	d := New(Id("test_dag")).Done()
	parents := d.TaskParents()
	if len(parents) != 0 {
		t.Errorf("Expected empty parents map for empty DAg, got: %v", parents)
	}
}

func TestDagTaskParentsLL(t *testing.T) {
	const N = 100
	g := linkedList(N)
	d := New(Id("test_dag")).AddRoot(g).Done()
	parents := d.TaskParents()

	if len(parents) != N {
		t.Errorf("Expected %d tasks in parents map, got: %d", N, len(parents))
	}

	startPar, exists := parents["Start"]
	if !exists {
		t.Error("Start should be the root, but does not exist in parents map")
	}
	if len(startPar) != 0 {
		t.Errorf("Start should not have parents but got: %d (%v)",
			len(startPar), startPar)
	}

	for taskId, par := range parents {
		if taskId == "Start" {
			continue
		}
		if len(par) != 1 {
			t.Errorf("Task %s should have only 1 parent, got: %d (%v)",
				taskId, len(par), par)
		}
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

func TestRegistryAddUnique(t *testing.T) {
	r := make(Registry)
	sched := FixedSchedule{Start: time.Now(), Interval: 1 * time.Second}
	dags := []Dag{
		New(Id("dag1")).Done(),
		New(Id("dag2")).AddAttributes(Attr{}).Done(),
		New(Id("dag3")).AddSchedule(&sched).Done(),
	}

	for _, d := range dags {
		addErr := r.Add(d)
		if addErr != nil {
			t.Errorf("Unexpected error while adding DAG %+v: %s",
				d, addErr.Error())
		}
	}

	for _, d := range dags {
		if _, exist := r[d.Id]; !exist {
			t.Errorf("Expected DAG %s to exist in registry, but it does not",
				string(d.Id))
		}
	}
}

func TestRegistryAddDuplicate(t *testing.T) {
	r := make(Registry)
	sched := FixedSchedule{Start: time.Now(), Interval: 1 * time.Second}
	dags := []Dag{
		New(Id("dag1")).Done(),
		New(Id("dag2")).AddAttributes(Attr{}).Done(),
		New(Id("dag3")).AddSchedule(&sched).Done(),
	}

	duplicates := []Dag{
		New(Id("dag3")).Done(),
		New(Id("dag1")).Done(),
		New(Id("dag2")).Done(),
	}

	for _, d := range dags {
		addErr := r.Add(d)
		if addErr != nil {
			t.Errorf("Unexpected error while adding DAG %+v: %s",
				d, addErr.Error())
		}
	}

	for _, d := range duplicates {
		addErr := r.Add(d)
		if addErr == nil {
			t.Errorf("Expected non-nil error for duplicated DAG ID, but got nil for DAG %s",
				string(d.Id))
		}
	}

	for _, d := range dags {
		if _, exist := r[d.Id]; !exist {
			t.Errorf("Expected DAG %s to exist in registry, but it does not",
				string(d.Id))
		}
	}
}
