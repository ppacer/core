package dag

import (
	"fmt"
	"testing"
)

type constTask struct{}

func (tt constTask) Id() string { return "constTask CONST" }
func (tt constTask) Execute()   { fmt.Println("Executing...") }

type emptyTask struct{}

func (et emptyTask) Id() string { return "EmptyTask" }
func (et emptyTask) Execute()   {}

func TestExecSourceEmpty(t *testing.T) {
	etask := emptyTask{}
	etaskSource := TaskExecuteSource(etask)
	const expectedExecSource = `{
}`
	if etaskSource != expectedExecSource {
		t.Errorf("Expected emptyTask.Execute source code to be [%s], but got: [%s]",
			expectedExecSource, etaskSource)
	}
}

func TestExecSourceConst(t *testing.T) {
	ctask := constTask{}
	ctaskSource := TaskExecuteSource(ctask)
	const expectedExecSource = `{
	fmt.Println("Executing...")
}`
	if ctaskSource != expectedExecSource {
		t.Errorf("Expected constTask.Execute source code to be [%s], but got: [%s]",
			expectedExecSource, ctaskSource)
	}
}
