package dag

import (
	"fmt"
	"testing"
)

type constTask struct{}

func (tt constTask) Id() string { return "ConstTask" }
func (tt constTask) Execute()   { fmt.Println("Executing...") }

type emptyTask struct{}

func (et emptyTask) Id() string { return "EmptyTask" }
func (et emptyTask) Execute()   {}

type aTask struct{}

func (at aTask) Id() string { return "A" }
func (at aTask) Execute()   { fmt.Println("A") }

type bTask struct{}

func (bt bTask) Id() string { return "B" }
func (bt bTask) Execute()   { fmt.Println("B") }

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

func TestJointTasksExecSources(t *testing.T) {
	n1 := Node{Task: constTask{}}
	n2 := Node{Task: constTask{}}
	n3 := Node{Task: constTask{}}
	n1.Next(&n2)
	n2.Next(&n3)

	execSources := n1.joinTasksExecSources([]byte{})
	expectedExecSources := `ConstTask:{
	fmt.Println("Executing...")
}ConstTask:{
	fmt.Println("Executing...")
}ConstTask:{
	fmt.Println("Executing...")
}`
	if string(execSources) != expectedExecSources {
		t.Errorf("Expected %s, but got %s", expectedExecSources, string(execSources))
	}
}

func TestJointTasksExecSourcesBroad(t *testing.T) {
	//       n21 -- n3
	//     /
	//    /
	// n1 -- n22
	//    \
	//     \
	//       n23
	n1 := Node{Task: constTask{}}
	n21 := Node{Task: aTask{}}
	n22 := Node{Task: bTask{}}
	n23 := Node{Task: constTask{}}
	n3 := Node{Task: emptyTask{}}

	n1.Next(&n21)
	n1.Next(&n22)
	n1.Next(&n23)
	n21.Next(&n3)

	execSources := n1.joinTasksExecSources([]byte{})
	expectedExecSources := `ConstTask:{
	fmt.Println("Executing...")
}A:{
	fmt.Println("A")
}EmptyTask:{
}B:{
	fmt.Println("B")
}ConstTask:{
	fmt.Println("Executing...")
}`
	if string(execSources) != expectedExecSources {
		t.Errorf("Expected %s, but got %s", expectedExecSources, string(execSources))
	}
}

// TODO: tests for hashing
