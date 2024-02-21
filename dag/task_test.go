// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package dag

import (
	"embed"
	"fmt"
	"os"
	"testing"

	"github.com/ppacer/core/meta"
)

type constTask struct{}

func (tt constTask) Id() string            { return "ConstTask" }
func (tt constTask) Execute(_ TaskContext) { fmt.Println("Executing...") }

type emptyTask struct{}

func (et emptyTask) Id() string            { return "EmptyTask" }
func (et emptyTask) Execute(_ TaskContext) {}

type aTask struct{}

func (at aTask) Id() string            { return "A" }
func (at aTask) Execute(_ TaskContext) { fmt.Println("A") }

type bTask struct{}

func (bt bTask) Id() string            { return "B" }
func (bt bTask) Execute(_ TaskContext) { fmt.Println("B") }

type nameTask struct {
	Name string
}

func (nt nameTask) Id() string            { return nt.Name }
func (nt nameTask) Execute(_ TaskContext) { fmt.Println(nt.Name) }

//go:embed *.go
var goSourceFiles embed.FS

func TestMain(m *testing.M) {
	meta.ParseASTs(goSourceFiles)
	os.Exit(m.Run())
}

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

// TODO(dskrzypiec): Unit tests for TaskHash

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
	g := linkedList(MAX_RECURSION + 2)
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

func TestIsAcyclicOnBranchoutAndJoin(t *testing.T) {
	g := branchOutAndMergeGraph()
	if !g.isAcyclic() {
		t.Error("Expected branch-out-and-merge graph to be acyclic, but is not")
	}
}

func TestFlattenSimple(t *testing.T) {
	g := deep3Width3Graph()
	nodesInfo := g.Flatten()

	if len(nodesInfo) != 5 {
		t.Errorf("Expected 5 tasks, got: %d", len(nodesInfo))
	}

	expectedIds := []string{"ConstTask", "A", "B", "ConstTask", "EmptyTask"}
	expectedLevels := []int{1, 2, 2, 2, 3}
	expectedNumOfParents := []int{0, 1, 1, 1, 1}
	for idx, ni := range nodesInfo {
		if ni.Node.Task.Id() != expectedIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s",
				idx, expectedIds[idx], ni.Node.Task.Id())
		}
		if ni.Depth != expectedLevels[idx] {
			t.Errorf("Expected task %s to be on depth %d, but got %d",
				ni.Node.Task.Id(), expectedLevels[idx], ni.Depth)
		}
		if len(ni.Parents) != expectedNumOfParents[idx] {
			t.Errorf("Expected task %s to has %d parents, got %d",
				ni.Node.Task.Id(), expectedNumOfParents[idx], len(ni.Parents))
		}
	}
}

func TestFlattenLinkedList(t *testing.T) {
	const N = 100
	l := linkedList(N)
	nodesInfo := l.Flatten()

	if len(nodesInfo) != N {
		t.Errorf("Expected flatten %d tasks, got: %d", N, len(nodesInfo))
	}

	var expectedTaskIds []string
	expectedTaskIds = append(expectedTaskIds, "Start")
	for i := 0; i < N-1; i++ {
		expectedTaskIds = append(expectedTaskIds, fmt.Sprintf("step_%d", i))
	}

	for idx, ni := range nodesInfo {
		if ni.Node.Task.Id() != expectedTaskIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s",
				idx, expectedTaskIds[idx], ni.Node.Task.Id())
		}
		if idx == 0 && len(ni.Parents) != 0 {
			t.Errorf("Expected 0 parents for the first node, but got: %d",
				len(ni.Parents))
		}
		if idx > 0 && len(ni.Parents) != 1 {
			t.Errorf("Expected 1 parent for a node inside linked list, got %d",
				len(ni.Parents))
		}
		if ni.Depth != idx+1 {
			t.Errorf("Expected Task %s to be on depth %d, got %d",
				ni.Node.Task.Id(), idx+1, ni.Depth)
		}
	}
}

func TestFlattenBinaryTree(t *testing.T) {
	g := binaryTree(2)
	nodesInfo := g.Flatten()

	expectedTaskIds := []string{
		"Node", "Node_0", "Node_1", "Node_0_0", "Node_0_1", "Node_1_0",
		"Node_1_1",
	}
	expectedLevels := []int{1, 2, 2, 3, 3, 3, 3}
	expectedNumOfParents := []int{0, 1, 1, 1, 1, 1, 1}
	for idx, ni := range nodesInfo {
		if ni.Node.Task.Id() != expectedTaskIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s", idx,
				expectedTaskIds[idx], ni.Node.Task.Id())
		}
		if ni.Depth != expectedLevels[idx] {
			t.Errorf("Expected task %s to be on depth %d, but got %d",
				ni.Node.Task.Id(), expectedLevels[idx], ni.Depth)
		}
		if len(ni.Parents) != expectedNumOfParents[idx] {
			t.Errorf("Expected task %s to has %d parents, got %d",
				ni.Node.Task.Id(), expectedNumOfParents[idx], len(ni.Parents))
		}
	}
}

func TestFlattenBranchoutAndMerge(t *testing.T) {
	g := branchOutAndMergeGraph()
	nodesInfo := g.Flatten()
	expectedTaskIds := []string{"n1", "n21", "n22", "n23", "n3"}
	expectedLevels := []int{1, 2, 2, 2, 3}
	expectedNumOfParents := []int{0, 1, 1, 1, 3}

	if len(nodesInfo) != len(expectedTaskIds) {
		t.Errorf("Expected flatten %d tasks, but got %d",
			len(expectedTaskIds), len(nodesInfo))
		return
	}
	for idx, ni := range nodesInfo {
		if ni.Node.Task.Id() != expectedTaskIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s",
				idx, expectedTaskIds[idx], ni.Node.Task.Id())
		}
		if ni.Depth != expectedLevels[idx] {
			t.Errorf("Expected task %s to be on depth %d, but got %d",
				ni.Node.Task.Id(), expectedLevels[idx], ni.Depth)
		}
		if len(ni.Parents) != expectedNumOfParents[idx] {
			t.Errorf("Expected task %s to has %d parents, got %d",
				ni.Node.Task.Id(), expectedNumOfParents[idx], len(ni.Parents))
		}
	}

	n3ExpectedParentIds := []string{"n21", "n22", "n23"}
	for idx, parent := range nodesInfo[len(nodesInfo)-1].Parents {
		if parent.Task.Id() != n3ExpectedParentIds[idx] {
			t.Errorf("n3 parent %d expected to have ID=%s, but got: %s", idx,
				n3ExpectedParentIds[idx], parent.Task.Id())
		}
	}
}

func TestFlattenFewBranchoutsAndMerge(t *testing.T) {
	g := fewBranchoutsAndMergesGraph()
	nodesInfo := g.Flatten()

	expectedTaskIds := []string{
		"n1",
		"g1n1", "g3", "g2n1",
		"g1n21", "g1n22", "g2n21", "g2n22", "g2n23",
		"g1n3", "g2n31", "g2n32",
		"g2Merge",
		"finish",
	}
	expectedLevels := []int{
		1,
		2, 2, 2,
		3, 3, 3, 3, 3,
		4, 4, 4,
		5,
		6,
	}
	expectedNumOfParents := []int{
		0,
		1, 1, 1,
		1, 1, 1, 1, 1,
		2, 2, 1,
		2,
		3,
	}

	if len(nodesInfo) != len(expectedTaskIds) {
		t.Errorf("Expected flatten %d tasks, but got %d",
			len(expectedTaskIds), len(nodesInfo))
		return
	}
	for idx, ni := range nodesInfo {
		if ni.Node.Task.Id() != expectedTaskIds[idx] {
			t.Errorf("For task %d expected ID=%s, got: %s", idx,
				expectedTaskIds[idx], ni.Node.Task.Id())
		}
		if ni.Depth != expectedLevels[idx] {
			t.Errorf("Expected task %s to be on depth %d, but got %d",
				ni.Node.Task.Id(), expectedLevels[idx], ni.Depth)
		}
		if len(ni.Parents) != expectedNumOfParents[idx] {
			t.Errorf("Expected task %s to has %d parents, got %d",
				ni.Node.Task.Id(), expectedNumOfParents[idx], len(ni.Parents))
		}
	}

	finishExpectedParentIds := []string{"g3", "g1n3", "g2Merge"}
	for idx, parent := range nodesInfo[len(nodesInfo)-1].Parents {
		if parent.Task.Id() != finishExpectedParentIds[idx] {
			t.Errorf("finish parent %d expected to have ID=%s, but got: %s",
				idx, finishExpectedParentIds[idx], parent.Task.Id())
		}
	}
}

func TestDepthSingleNode(t *testing.T) {
	n := Node{Task: nameTask{Name: "Start"}}
	if n.depth() != 1 {
		t.Errorf("Expected single node to have depth=1, got: %d", n.depth())
	}
}

func TestDepthBinary(t *testing.T) {
	const N = 5
	g := binaryTree(N)
	if g.depth() != N+1 {
		t.Errorf("Expected tree depth to be %d, but got: %d", N+1, g.depth())
	}
}

func TestDepthLinkedList(t *testing.T) {
	const N = 500
	g := linkedList(N)
	if g.depth() != N {
		t.Errorf("Expected tree depth to be %d, but got: %d", N, g.depth())
	}
}

func TestDepthLinkedLong(t *testing.T) {
	const N = MAX_RECURSION * 2
	g := linkedList(N)
	if g.depth() != N {
		t.Errorf("Expected long tree depth to be %d, but got: %d", N, g.depth())
	}
}

func TestDepthBranchoutAndJoin(t *testing.T) {
	g := branchOutAndMergeGraph()
	if g.depth() != 3 {
		t.Errorf("Expected branch-out-and-merge graph to has depth=3, got: %d",
			g.depth())
	}
}

func TestJointTasksExecSources(t *testing.T) {
	n1 := Node{Task: constTask{}}
	n2 := Node{Task: constTask{}}
	n3 := Node{Task: constTask{}}
	n1.Next(&n2)
	n2.Next(&n3)

	execSources := n1.joinTasksExecSources()
	expectedExecSources := `ConstTask:{
	fmt.Println("Executing...")
}ConstTask:{
	fmt.Println("Executing...")
}ConstTask:{
	fmt.Println("Executing...")
}`
	if string(execSources) != expectedExecSources {
		t.Errorf("Expected %s, but got %s", expectedExecSources,
			string(execSources))
	}
}

func TestJointTasksExecSourcesBroad(t *testing.T) {
	n1 := deep3Width3Graph()
	execSources := n1.joinTasksExecSources()
	expectedExecSources := `ConstTask:{
	fmt.Println("Executing...")
}A:{
	fmt.Println("A")
}B:{
	fmt.Println("B")
}ConstTask:{
	fmt.Println("Executing...")
}EmptyTask:{
}`
	if string(execSources) != expectedExecSources {
		t.Errorf("Expected %s, but got %s", expectedExecSources,
			string(execSources))
	}
}

// It's exactly the same as aTask to test hashing
type aTaskCopy struct{}

func (at aTaskCopy) Id() string            { return "A" }
func (at aTaskCopy) Execute(_ TaskContext) { fmt.Println("A") }

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

// It's exactly the same as aTask but differes in one char in Execute
// implementation.
type aTaskWithSpace struct{}

func (at aTaskWithSpace) Id() string            { return "A" }
func (at aTaskWithSpace) Execute(_ TaskContext) { fmt.Println("A ") }

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

func (at aTaskDifferentId) Id() string            { return "Not A" }
func (at aTaskDifferentId) Execute(_ TaskContext) { fmt.Println("A ") }

func TestNodeTheSameExecute(t *testing.T) {
	n1 := Node{Task: aTask{}}
	n2 := Node{Task: aTaskDifferentId{}}
	h1 := n1.Hash()
	h2 := n2.Hash()

	if h1 == h2 {
		t.Error("Expected different hashes for aTask and aTaskWithSpace but got the same")
	}
}

//	   n21
//	 /    \
//	/      \
//
// n1 - n22 - n3
//
//	\      /
//	 \    /
//	   n23
func branchOutAndMergeGraph() *Node {
	n1 := nameTaskNode("n1")
	n21 := nameTaskNode("n21")
	n22 := nameTaskNode("n22")
	n23 := nameTaskNode("n23")
	n3 := nameTaskNode("n3")

	n1.NextAsyncAndMerge([]*Node{n21, n22, n23}, n3)
	return n1
}

//	        g1n21
//	      /       \
//	   g1n1       g1n3 -----------
//	   /  \       /               \
//	  /     g1n21                  \
//	 /                              \
//	/                                \
//
// n1---g3---------------------------finish
//
//	\                                /
//	 \      g2n21                   /
//	  \    /     \                 /
//	   \  /       \               /
//	   g2n1--g2n22-g2n31         /
//	      \              \      /
//	       \             g2Merge
//	        \            /
//	         g2n23--g2n32
func fewBranchoutsAndMergesGraph() *Node {
	n1 := nameTaskNode("n1")

	// sub graph 1
	g1n1 := nameTaskNode("g1n1")
	g1n21 := nameTaskNode("g1n21")
	g1n22 := nameTaskNode("g1n22")
	g1n3 := nameTaskNode("g1n3")

	n1.Next(g1n1)
	g1n1.NextAsyncAndMerge([]*Node{g1n21, g1n22}, g1n3)

	// sub graph 3
	g3 := nameTaskNode("g3")
	n1.Next(g3)

	// sub graph 2
	g2n1 := nameTaskNode("g2n1")
	g2n21 := nameTaskNode("g2n21")
	g2n22 := nameTaskNode("g2n22")
	g2n23 := nameTaskNode("g2n23")
	g2n31 := nameTaskNode("g2n31")
	g2n32 := nameTaskNode("g2n32")
	g2Merge := nameTaskNode("g2Merge")

	n1.Next(g2n1)
	g2n1.NextAsyncAndMerge([]*Node{g2n21, g2n22}, g2n31)
	g2n1.Next(g2n23)
	g2n23.Next(g2n32)
	g2n31.Next(g2Merge)
	g2n32.Next(g2Merge)

	finish := nameTaskNode("finish")
	g1n3.Next(finish)
	g3.Next(finish)
	g2Merge.Next(finish)

	return n1
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

func nameTaskNode(name string) *Node {
	return &Node{Task: nameTask{Name: name}}
}

func linkedList(length int) *Node {
	s := Node{Task: nameTask{Name: "Start"}}
	prev := &s
	for i := 0; i < length-1; i++ {
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
