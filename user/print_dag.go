package user

import (
	"time"

	"github.com/dskrzypiec/scheduler/dag"
	"github.com/dskrzypiec/scheduler/user/tasks"
)

func createPrintDag() dag.Dag {
	root := dag.Node{Task: tasks.PrintTask{Name: "start"}}
	end := dag.Node{Task: tasks.PrintTask{Name: "end"}}

	// say_hello --> end
	root.Next(&end)

	startTs := time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 3 * time.Second, Start: startTs}
	attr := dag.Attr{Tags: []string{"test"}}
	d := dag.New(dag.Id("print_dag")).
		AddSchedule(&sched).
		AddRoot(&root).
		AddAttributes(attr).
		Done()
	return d
}
