package tasks

import "fmt"

type PrintTask struct {
	Name string
}

func (pt PrintTask) Id() string { return pt.Name }

func (pt PrintTask) Execute() {
	fmt.Println("Hello executor!")
}
