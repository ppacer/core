package user

import (
	"go_shed/src/dag"
	"go_shed/src/user/tasks"
	"time"
)

func createHelloDag() dag.Dag {
	root := dag.Node{Task: tasks.PrintTask{Name: "say_hello"}}
	wait10s := dag.Node{
		Task: tasks.WaitTask{TaskId: "wait_10", Interval: 10 * time.Second},
	}
	wait5s := dag.Node{
		Task: tasks.WaitTask{TaskId: "wait_5", Interval: 5 * time.Second},
	}
	end := dag.Node{Task: tasks.PrintTask{Name: "end"}}

	//            wait_10 ---
	//           /           \
	// say_hello              -- end
	//           \           /
	//            wait_5 ----
	root.Next(&wait10s)
	root.Next(&wait5s)
	wait10s.Next(&end)
	wait5s.Next(&end)

	startTs := time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	sched := dag.IntervalSchedule{Interval: 1 * time.Minute, Start: startTs}
	attr := dag.Attr{Tags: []string{"test"}}
	d := dag.New(dag.Id("hello_dag")).
		AddSchedule(&sched).
		AddRoot(&root).
		AddAttributes(attr).
		Done()
	return d
}
