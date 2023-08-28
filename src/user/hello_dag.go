package user

import (
	"go_shed/src/dag"
	"go_shed/src/user/tasks"
	"time"
)

func createHelloDag() dag.Dag {
	attrs := dag.Attr{
		Id:       dag.Id("hello_dag"),
		Schedule: "* * * * *",
	}
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

	return dag.New(attrs, &root)
}
