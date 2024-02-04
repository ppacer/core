package e2etests

import (
	"fmt"

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

func newLinkedListWriteToSliceDAG(
	dagId dag.Id, size int, sched *dag.Schedule,
) (dag.Dag, []string) {
	data := make([]string, 0, 10)
	t0 := writeToSliceTask{taskId: "task_0", data: data}
	s := dag.Node{Task: &t0}
	prev := &s

	for i := 1; i < size; i++ {
		t := writeToSliceTask{taskId: fmt.Sprintf("task_%d", i), data: data}
		n := dag.Node{Task: &t}
		prev.Next(&n)
		prev = &n
	}

	d := dag.New(dagId).AddRoot(&s)
	if sched != nil {
		d.AddSchedule(*sched)
	}
	return d.Done(), data
}
