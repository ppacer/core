package e2etests

import (
	"fmt"
	"time"

	"github.com/ppacer/core/dag"
)

func simple131DAG(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.Node{Task: emptyTask{taskId: "n1"}}
	n21 := dag.Node{Task: emptyTask{taskId: "n21"}}
	n22 := dag.Node{Task: emptyTask{taskId: "n22"}}
	n23 := dag.Node{Task: emptyTask{taskId: "n23"}}
	n3 := dag.Node{Task: emptyTask{taskId: "n3"}}
	n1.NextAsyncAndMerge([]*dag.Node{&n21, &n22, &n23}, &n3)

	d := dag.New(dagId)
	d.AddRoot(&n1)
	d.AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithErrTask(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.Node{Task: emptyTask{taskId: "start"}}
	n2 := dag.Node{Task: errTask{taskId: "task1"}}
	n3 := dag.Node{Task: emptyTask{taskId: "end"}}
	n1.Next(&n2)
	n2.Next(&n3)

	d := dag.New(dagId).AddRoot(&n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleDAGWithRuntimeErrTask(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.Node{Task: emptyTask{taskId: "start"}}
	n2 := dag.Node{Task: runtimeErrTask{taskId: "task1"}}
	n3 := dag.Node{Task: emptyTask{taskId: "end"}}
	n1.Next(&n2)
	n2.Next(&n3)

	d := dag.New(dagId).AddRoot(&n1)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func simpleLoggingDAG(dagId dag.Id, sched *dag.Schedule) dag.Dag {
	n1 := dag.Node{Task: logTask{taskId: "n1"}}
	n21 := dag.Node{Task: logTask{taskId: "n21"}}
	n22 := dag.Node{Task: logTask{taskId: "n22"}}
	n3 := dag.Node{Task: logTask{taskId: "n3"}}
	n1.NextAsyncAndMerge([]*dag.Node{&n21, &n22}, &n3)

	d := dag.New(dagId)
	d.AddRoot(&n1)
	d.AddAttributes(dag.Attr{CatchUp: false})

	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func linkedListEmptyTasksDAG(
	dagId dag.Id, size int, sched *dag.Schedule,
) dag.Dag {
	t0 := emptyTask{taskId: "task_0"}
	s := dag.Node{Task: &t0}
	prev := &s

	for i := 1; i < size; i++ {
		t := emptyTask{taskId: fmt.Sprintf("task_%d", i)}
		n := dag.Node{Task: &t}
		prev.Next(&n)
		prev = &n
	}

	d := dag.New(dagId).AddRoot(&s)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}

func linkedListWaitTasksDAG(
	dagId dag.Id, size int, interval time.Duration, sched *dag.Schedule,
) dag.Dag {
	t0 := waitTask{taskId: "task_0", interval: interval}
	s := dag.Node{Task: &t0}
	prev := &s

	for i := 1; i < size; i++ {
		t := waitTask{taskId: fmt.Sprintf("task_%d", i), interval: interval}
		n := dag.Node{Task: &t}
		prev.Next(&n)
		prev = &n
	}

	d := dag.New(dagId).AddRoot(&s)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done()
}
