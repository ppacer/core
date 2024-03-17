// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package e2etests contains end-to-end tests for ppacer.
package e2etests

import (
	"fmt"
	"time"

	"github.com/ppacer/core/dag"
)

func simple131DAG(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "n1"})
	n21 := dag.NewNode(emptyTask{taskId: "n21"})
	n22 := dag.NewNode(emptyTask{taskId: "n22"})
	n23 := dag.NewNode(emptyTask{taskId: "n23"})
	n3 := dag.NewNode(emptyTask{taskId: "n3"})
	n1.NextAsyncAndMerge([]*dag.Node{n21, n22, n23}, n3)

	d := dag.New(dagId)
	d.AddRoot(n1)
	d.AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithErrTask(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "start"})
	n2 := dag.NewNode(errTask{taskId: "task1"})
	n3 := dag.NewNode(emptyTask{taskId: "end"})
	n1.Next(n2).Next(n3)

	d := dag.New(dagId).AddRoot(n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithRuntimeErrTask(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "start"})
	n2 := dag.NewNode(runtimeErrTask{taskId: "task1"})
	n3 := dag.NewNode(emptyTask{taskId: "end"})
	n1.Next(n2).Next(n3)

	d := dag.New(dagId).AddRoot(n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleLoggingDAG(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.NewNode(logTask{taskId: "n1"})
	n21 := dag.NewNode(logTask{taskId: "n21"})
	n22 := dag.NewNode(logTask{taskId: "n22"})
	n3 := dag.NewNode(logTask{taskId: "n3"})
	n1.NextAsyncAndMerge([]*dag.Node{n21, n22}, n3)

	d := dag.New(dagId)
	d.AddRoot(n1)
	d.AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func linkedListEmptyTasksDAG(
	dagId dag.Id, size int, sched *dag.Schedule,
) dag.Dag {
	s := dag.NewNode(emptyTask{taskId: "task_0"})
	prev := s

	for i := 1; i < size; i++ {
		n := dag.NewNode(emptyTask{taskId: fmt.Sprintf("task_%d", i)})
		prev = prev.Next(n)
	}

	d := dag.New(dagId).AddRoot(s)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func linkedListWaitTasksDAG(
	dagId dag.Id, size int, interval time.Duration, sched *dag.Schedule,
) dag.Dag {
	s := dag.NewNode(waitTask{taskId: "task_0", interval: interval})
	prev := s

	for i := 1; i < size; i++ {
		t := waitTask{taskId: fmt.Sprintf("task_%d", i), interval: interval}
		n := dag.NewNode(t)
		prev = prev.Next(n)
	}

	d := dag.New(dagId).AddRoot(s)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}
