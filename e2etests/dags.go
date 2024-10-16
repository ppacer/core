// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package e2etests contains end-to-end tests for ppacer.
package e2etests

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/notify"
)

func singleEmptyTaskDag(dagId dag.Id, sched schedule.Schedule) dag.Dag {
	root := dag.NewNode(emptyTask{taskId: "root"})
	return dag.New(dagId).
		AddRoot(root).
		AddAttributes(dag.Attr{CatchUp: false}).
		AddSchedule(sched).
		Done()
}

func simpleDAGWithLongRunningTasks(
	dagId dag.Id, sched schedule.Schedule, execDuration time.Duration,
	taskTimeoutDuration time.Duration, notifications *[]string,
) dag.Dag {
	notifier := notify.NewMock(notifications)

	n1 := dag.NewNode(emptyTask{taskId: "start"})
	n2 := dag.NewNode(
		waitTask{taskId: "task1", interval: execDuration},
		dag.WithTaskTimeout(taskTimeoutDuration),
		dag.WithCustomNotifier(notifier),
	)
	n3 := dag.NewNode(emptyTask{taskId: "end"})
	n1.Next(n2).Next(n3)

	d := dag.New(dagId).AddRoot(n1)
	if sched != nil {
		d.AddSchedule(sched)
	}
	return d.Done()
}

func manyParallelLongRunningsTasksDag(
	n int, dagId dag.Id, sched schedule.Schedule, execDuration time.Duration,
	taskTimeoutDuration time.Duration, notifications *[]string,
) dag.Dag {
	notifier := notify.NewMock(notifications)
	start := dag.NewNode(emptyTask{taskId: "start"})
	finish := dag.NewNode(emptyTask{taskId: "finish"})

	asyncNodes := make([]*dag.Node, 0, n)
	for i := 0; i < n; i++ {
		taskId := fmt.Sprintf("task_%d", i+1)
		tmp := dag.NewNode(
			waitTask{taskId: taskId, interval: execDuration},
			dag.WithTaskTimeout(taskTimeoutDuration),
			dag.WithCustomNotifier(notifier),
		)
		asyncNodes = append(asyncNodes, tmp)
	}
	start.NextAsyncAndMerge(asyncNodes, finish)

	return dag.New(dagId).
		AddRoot(start).
		AddSchedule(sched).
		Done()
}

func simple131DAG(dagId dag.Id, sched *schedule.Schedule) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "n1"})
	n21 := dag.NewNode(emptyTask{taskId: "n21"})
	n22 := dag.NewNode(emptyTask{taskId: "n22"})
	n23 := dag.NewNode(emptyTask{taskId: "n23"})
	n3 := dag.NewNode(emptyTask{taskId: "n3"})
	n1.NextAsyncAndMerge([]*dag.Node{n21, n22, n23}, n3)

	d := dag.New(dagId).
		AddRoot(n1).
		AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithErrTask(dagId dag.Id, sched *schedule.Schedule) dag.Dag {
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

func simpleDAGWithErrTaskCustomNotifier(
	dagId dag.Id, sched *schedule.Schedule, notifier notify.Sender,
) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "start"})
	n2 := dag.NewNode(
		errTask{taskId: "task1"},
		dag.WithCustomNotifier(notifier),
	)
	n3 := dag.NewNode(emptyTask{taskId: "end"})
	n1.Next(n2).Next(n3)

	d := dag.New(dagId).AddRoot(n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithRuntimeErrTask(dagId dag.Id, sched *schedule.Schedule) dag.Dag {
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

func simpleDAGWithTaskConfigFuncs(
	dagId dag.Id, sched *schedule.Schedule, failedRuns int,
	taskConfigFuncs ...dag.TaskConfigFunc,
) dag.Dag {
	n1 := dag.NewNode(emptyTask{taskId: "start"})
	n2 := dag.NewNode(
		&failNTimesTask{taskId: "task1", n: failedRuns},
		taskConfigFuncs...,
	)
	n3 := dag.NewNode(emptyTask{taskId: "end"})
	n1.Next(n2).Next(n3)

	d := dag.New(dagId).AddRoot(n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func singleFailingTaskDAG(
	dagId dag.Id, sched *schedule.Schedule, failedRuns int,
	taskConfigFuncs ...dag.TaskConfigFunc,
) dag.Dag {
	root := dag.NewNode(
		&failNTimesTask{taskId: "root", n: failedRuns},
		taskConfigFuncs...,
	)
	d := dag.New(dagId).AddRoot(root)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleLoggingDAG(dagId dag.Id, sched *schedule.Schedule) dag.Dag {
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
	dagId dag.Id, size int, sched *schedule.Schedule,
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
	dagId dag.Id, size int, interval time.Duration, sched *schedule.Schedule,
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

type customNotifier struct {
	phrase string
	buf    *[]string
}

func newCustomNotifier(phrase string, buffor *[]string) *customNotifier {
	return &customNotifier{
		phrase: phrase,
		buf:    buffor,
	}
}

func (cn *customNotifier) Send(_ context.Context, tmpl notify.Template, data notify.MsgData) error {
	var msgBuff bytes.Buffer
	_, w1Err := msgBuff.WriteString(fmt.Sprintf("%s: ", cn.phrase))
	if w1Err != nil {
		return w1Err
	}
	writeErr := tmpl.Execute(&msgBuff, data)
	if writeErr != nil {
		return writeErr
	}
	*cn.buf = append(*cn.buf, msgBuff.String())
	return nil
}
