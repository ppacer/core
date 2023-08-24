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

type nameTask struct {
	Name string
}

func (nt nameTask) Id() string { return nt.Name }
func (nt nameTask) Execute()   { fmt.Println(nt.Name) }

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

func TestIsAcyclicSimple(t *testing.T) {
	g := deep3Width3Graph()
	if !g.isAcyclic() {
		t.Error("Expected graph to be acylic, but is not")
	}
}

func TestIsAcyclicLongList(t *testing.T) {
	g := linkedList(500)
	if !g.isAcyclic() {
		t.Error("Expected graph to be acylic, but is not")
	}
}

func TestIsAcyclicTooLongList(t *testing.T) {
	g := linkedList(MAX_RECURSION + 1)
	if g.isAcyclic() {
		t.Error("Expected isAcyclic to be false on too deep graphs")
	}
}

func TestIsAcyclicOnBinaryTree(t *testing.T) {
	g := binaryTree(4)
	if !g.isAcyclic() {
		t.Error("Expected binary tree to be acylic, but is not")
	}
}

func TestIsAcyclicOnCyclicSimple(t *testing.T) {
	n1 := Node{Task: constTask{}}
	n2 := Node{Task: constTask{}}
	n3 := Node{Task: constTask{}}
	n1.Next(&n2)
	n2.Next(&n3)
	n3.Next(&n1)

	if n1.isAcyclic() {
		t.Error("Expected graph to be cylic, but isAcyclic says otherwise")
	}
}

func TestFlattenBfsSimple(t *testing.T) {
	g := deep3Width3Graph()
	tasks := g.flatten(true)
	for _, t := range tasks {
		fmt.Println(t.Id())
	}

	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got: %d", len(tasks))
	}

	expectedIds := []string{"ConstTask", "A", "B", "ConstTask", "EmptyTask"}
	for idx, task := range tasks {
		if task.Id() != expectedIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s", idx, expectedIds[idx], task.Id())
		}
	}
}

func TestFlattenDfsSimple(t *testing.T) {
	g := deep3Width3Graph()
	tasks := g.flatten(false)
	for _, t := range tasks {
		fmt.Println(t.Id())
	}

	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got: %d", len(tasks))
	}

	expectedIds := []string{"ConstTask", "A", "EmptyTask", "B", "ConstTask"}
	for idx, task := range tasks {
		if task.Id() != expectedIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s", idx, expectedIds[idx], task.Id())
		}
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
	n1 := deep3Width3Graph()
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

func deep3Width3Graph() *Node {
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

	return &n1
}

func linkedList(length int) *Node {
	s := Node{Task: nameTask{Name: "Start"}}
	prev := &s
	for i := 0; i < length; i++ {
		n := Node{Task: nameTask{Name: fmt.Sprintf("step_%d", i)}}
		prev.Next(&n)
		prev = &n
	}
	return &s
}

func binaryTree(depth int) *Node {
	return balancedTree(depth, 2)
}

func balancedTree(depth int, childNum int) *Node {
	r := Node{Task: nameTask{Name: "Node"}}
	buildBalancedTree(&r, childNum, 1, depth)
	return &r
}

func buildBalancedTree(node *Node, n int, id, maxId int) {
	if id > maxId {
		return
	}
	addNChildren(node, n)
	for _, child := range node.Children {
		buildBalancedTree(child, n, id+1, maxId)
	}
}

func addNChildren(node *Node, n int) {
	for i := 0; i < n; i++ {
		n := Node{Task: nameTask{Name: fmt.Sprintf("%s_%d", node.Task.Id(), i)}}
		node.Next(&n)
	}
}
