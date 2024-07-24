// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

/*
Package dag provides DAG definition and related functionalities.

# Introduction

Package dag provides a way to express your business process in form of a DAG
(directed acyclic graph) using type Dag. Dag contains information about the
process schedule, attributes and actual graph of tasks.

# Creating new DAG

Recommended way to define a DAG is via fluent API provided by the package.
For example:

	task1 := NewNode(emptyTask{taskId: "task1"})
	task2 := NewNode(emptyTask{taskId: "task2"}, WithTaskRetries(3))
	task1.Next(task2)

	everyHour := schedule.NewCron().AtMinute(5)
	myDag := New(Id("sample_dag")).
		AddRoot(&task1).
		AddSchedule(everyHour).
		Done()

DAGs are usually stored in a single Registry (Id -> Dag mapping).

	dags := Registry{}
	addErr := dags.Add(myDag)

# Task configuration

Task configuration is kept on the Node level. This is because Task is all about
implementing Execute method and information about DAG structure and
configuration is kept on Node level. It’s a implementation detail though.
Node type contains Config field of type TaskConfig. The most
convenient way to setup custom task configuration is by creating a new node
using NewNode function.

	var task dag.Task = createSomeTask()

	// task with default config
	n1 := dag.NewNode(task)

	// task with 3 retries
	n2 := dag.NewNode(task, dag.WithTaskRetries(3))

	// task with 3 retries and 5 minutes delay between those retries
	n3 := dag.NewNode(

		task,
		dag.WithTaskRetries(3),
		dag.WithTaskRetriesDelay(5 * 60 * time.Second),

	)

	// when the following task would fail, then email notification would be sent
	var emailNotifier notify.Sender = setupEmailNotifier() // mock
	n4 := dag.NewNode(task, dag.WithCustomNotifier(emailNotifier))

	// setting up the same config for few tasks

		myTaskConfig := func(tc *TaskConfig) {
			tc.TimeoutSeconds = 60
			tc.Retries = 3
			tc.RetriesDelaySeconds = (1800 * time.Milliseconds).Seconds()
		}

	m1 := dag.NewNode(task1, myTaskConfig)
	m2 := dag.NewNode(task2, myTaskConfig)
	// ...
	mN := dag.NewNode(taskN, myTaskConfig)

# Reference

For more examples and high-level description, please visit:
https://ppacer.org/internals/dags/.
*/
package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/timeutils"
)

var ErrTaskNotFoundInDag = errors.New("task was not found in the DAG")
var ErrDagAlreadyInRegistry = errors.New("given DAG ID is already in the Registry")

// Dag represents a single process that can be scheduled by ppacer. It contains
// metadata of the process like identifiers, its schedule and a pointer to
// actual DAG of tasks to be executed within the process.
//
// Recommended way to create new Dag is to use provided fluent API:
//
//	myDag := New(Id("sample_dag")).
//	  AddSchedule(schedule.NewFixed(startTs, 60 * time.Second)).
//	  AddRoot(&root).
//	  AddAttributes(Attr{CatchUp: True}).
//	  Done()
type Dag struct {
	Id       Id
	Schedule *schedule.Schedule
	Attr     Attr
	Root     *Node
}

// DAG string identifier.
type Id string

// Attr represents additional attributes and parameters about Dag and its
// scheduling. This object is stored in the database as single value by
// serializing it to JSON.
type Attr struct {
	// If set to true schedule dag run would be catch up since last run or
	// Start.
	CatchUp bool     `json:"catchUp"`
	Tags    []string `json:"tags"`
}

// Registry for DAGs.
type Registry map[Id]Dag

// Add adds given DAG to registry. If there is already a DAG in registry with
// the same identifier as given DAG, then ErrDagAlreadyInRegistry is returned.
func (r Registry) Add(d Dag) error {
	if _, exists := r[d.Id]; exists {
		return ErrDagAlreadyInRegistry
	}
	r[d.Id] = d
	return nil
}

// New creates new Dag instance for given DAG identifier.
func New(id Id) *Dag {
	return &Dag{
		Id: id,
	}
}

// AddRoot set Dag Root to given node.
func (d *Dag) AddRoot(node *Node) *Dag {
	d.Root = node
	return d
}

// AddSchedule adds Dag schedule.
func (d *Dag) AddSchedule(sched schedule.Schedule) *Dag {
	d.Schedule = &sched
	return d
}

// AddAttributes adds Dag attributes.
func (d *Dag) AddAttributes(attr Attr) *Dag {
	d.Attr = attr
	return d
}

// Done returns self Dag instance. It's meant to that after calling this method
// our Dag is defined and shouldn't really by modified further.
func (d *Dag) Done() Dag {
	return *d
}

// Graph is a valid DAG when the following conditions are met:
//   - Is acyclic (does not have cycles)
//   - Task identifiers are unique within the graph
//   - Graph is no deeper then MAX_RECURSION
func (d *Dag) IsValid() bool {
	return d.Root.isAcyclic() && d.Root.taskIdsUnique() &&
		d.Root.depth() <= MAX_RECURSION
}

// GetTask return task by its identifier. In case when there is no Task within
// the DAG of given taskId, then non-nil error will be returned
// (ErrTaskNotFoundInDag).
func (d *Dag) GetTask(taskId string) (Task, error) {
	nodesInfo := d.Root.Flatten()
	for _, ni := range nodesInfo {
		if ni.Node.Task.Id() == taskId {
			return ni.Node.Task, nil
		}
	}
	return nil, ErrTaskNotFoundInDag
}

// GetNoe return DAG Node by task identifier. In case when there is no Task
// within the DAG of given taskId, then non-nil error will be returned
// (ErrTaskNotFoundInDag).
func (d *Dag) GetNode(taskId string) (*Node, error) {
	nodesInfo := d.Root.Flatten()
	for _, ni := range nodesInfo {
		if ni.Node.Task.Id() == taskId {
			return ni.Node, nil
		}
	}
	return nil, ErrTaskNotFoundInDag
}

// Flatten DAG into list of Tasks in BFS order.
func (d *Dag) Flatten() []Task {
	if d.Root == nil {
		return []Task{}
	}
	nodesInfo := d.Root.Flatten()
	tasks := make([]Task, len(nodesInfo))
	for idx, ni := range nodesInfo {
		tasks[idx] = ni.Node.Task
	}
	return tasks
}

// FlattenNodes flatten DAG into list of Nodes with enriched information in BFS
// order.
func (d *Dag) FlattenNodes() []NodeInfo {
	if d.Root == nil {
		return []NodeInfo{}
	}
	return d.Root.Flatten()
}

// TaskParents returns mapping of DAG task IDs onto its parents task IDs.
func (d *Dag) TaskParents() map[string][]string {
	if d.Root == nil {
		return map[string][]string{}
	}
	_, parentsNodeMap := d.Root.flattenBFS() // This does not include the root
	taskParents := make(map[string][]string, len(parentsNodeMap))
	taskParents[d.Root.Task.Id()] = []string{}

	for node, nodeParents := range parentsNodeMap {
		if node == nil {
			// This should not happen
			continue
		}
		parentTaskIds := make([]string, 0, len(nodeParents))
		for _, parent := range nodeParents {
			if parent != nil {
				parentTaskIds = append(parentTaskIds, parent.Task.Id())
			}
		}
		taskParents[node.Task.Id()] = parentTaskIds
	}
	return taskParents
}

// HashAttr calculates SHA256 hash based on DAG attribues, start time and
// schedule.
func (d *Dag) HashDagMeta() string {
	attrJson, jErr := json.Marshal(d.Attr)
	if jErr != nil {
		// very unlikely
		return "CANNOT SERIALIZE DAG ATTRIBUTES"
	}
	sched := ""
	startTsStr := ""
	if d.Schedule != nil {
		sched = (*d.Schedule).String()
		startTsStr = timeutils.ToString((*d.Schedule).Start())
	}

	hasher := sha256.New()
	hasher.Write(attrJson)
	hasher.Write([]byte(sched))
	hasher.Write([]byte(startTsStr))
	return hex.EncodeToString(hasher.Sum(nil))
}

// HashTasks calculates SHA256 hash based on concatanated body sources of
// Execute methods for all tasks.
func (d *Dag) HashTasks() string {
	if d.Root == nil {
		hasher := sha256.New()
		hasher.Write([]byte("NO TASKS"))
		return hex.EncodeToString(hasher.Sum(nil))
	}
	return d.Root.Hash()
}

// String return string with information about Dag Id, its schedule and tasks.
func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Id,
		(*d.Schedule).String(), d.Root.String(0))
}
