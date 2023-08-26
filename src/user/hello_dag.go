package user

import (
	"fmt"
	"go_shed/src/dag"
)

type PrintTask struct {
	Name string
}

func (pt PrintTask) Id() string { return pt.Name }

func (pt PrintTask) Execute() {
	fmt.Println("Hello executor!")
}

func createHelloDag() dag.Dag {
	attrs := dag.Attr{
		Id:       dag.Id("hello_dag"),
		Schedule: "* * * * *",
	}
	root := dag.Node{Task: PrintTask{Name: "say_hello"}}
	return dag.New(attrs, &root)
}
