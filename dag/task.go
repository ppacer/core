package dag

import (
	"fmt"
)

type Task interface {
	Id() string
	Execute()
}

type TTask struct{}

func (tt TTask) Id() string { return "TTask CONST" }
func (tt TTask) Execute()   { fmt.Println("Executing...") }
