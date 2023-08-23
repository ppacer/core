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

// It's exactly the same as aTask to test hashing
type aTaskCopy struct{}

func (at aTaskCopy) Id() string { return "A" }
func (at aTaskCopy) Execute()   { fmt.Println("A") }

func TestNodeHashesForSimilarNodes(t *testing.T) {
	n1 := Node{Task: aTask{}}
	n2 := Node{Task: aTaskCopy{}}
	h1 := n1.Hash()
	h2 := n2.Hash()

	if h1 != h2 {
		t.Errorf("Expected equal hashes, but got %s for aTask and %s for aTaskCopy."+
			"Source for aTask.Execute: %s, source for aTaskCopy.Execute: %s",
			h1, h2, TaskExecuteSource(n1.Task), TaskExecuteSource(n2.Task))
	}
}

// It's exactly the same as aTask but differes in one char in Execute implementation.
type aTaskWithSpace struct{}

func (at aTaskWithSpace) Id() string { return "A" }
func (at aTaskWithSpace) Execute()   { fmt.Println("A ") }

func TestNodeAlmostTheSame(t *testing.T) {
	n1 := Node{Task: aTask{}}
	n2 := Node{Task: aTaskWithSpace{}}
	h1 := n1.Hash()
	h2 := n2.Hash()

	if h1 == h2 {
		t.Error("Expected different hashes for aTask and aTaskWithSpace but got the same")
	}
}

type aTaskDifferentId struct{}

func (at aTaskDifferentId) Id() string { return "Not A" }
func (at aTaskDifferentId) Execute()   { fmt.Println("A ") }

func TestNodeTheSameExecute(t *testing.T) {
	n1 := Node{Task: aTask{}}
	n2 := Node{Task: aTaskDifferentId{}}
	h1 := n1.Hash()
	h2 := n2.Hash()

	if h1 == h2 {
		t.Error("Expected different hashes for aTask and aTaskWithSpace but got the same")
	}
}
