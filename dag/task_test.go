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

func (tt constTask) Id() string { return "ConstTask" }

func (tt constTask) Execute(_ TaskContext) error {
	fmt.Println("Executing...")
	return nil
}

type emptyTask struct{}

func (et emptyTask) Id() string { return "EmptyTask" }

func (et emptyTask) Execute(_ TaskContext) error {
	return nil
}

type aTask struct{}

func (at aTask) Id() string { return "A" }

func (at aTask) Execute(_ TaskContext) error {
	fmt.Println("A")
	return nil
}

type bTask struct{}

func (bt bTask) Id() string { return "B" }

func (bt bTask) Execute(_ TaskContext) error {
	fmt.Println("B")
	return nil
}

type nameTask struct {
	Name string
}

func (nt nameTask) Id() string { return nt.Name }

func (nt nameTask) Execute(_ TaskContext) error {
	fmt.Println(nt.Name)
	return nil
}

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
	return nil
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
	return nil
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
	n1 := NewNode(constTask{})
	n2 := NewNode(constTask{})
	n3 := NewNode(constTask{})
	n1.Next(n2).Next(n3).Next(n1)

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
		if ni.Width != 1 {
			t.Errorf("Expected Task %s to be on width %d, got %d",
				ni.Node.Task.Id(), 1, ni.Width)
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
	expectedWidths := []int{1, 1, 2, 1, 2, 3, 4}
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
		if ni.Width != expectedWidths[idx] {
			t.Errorf("Expected task %s to be on width %d, but got %d",
				ni.Node.Task.Id(), expectedWidths[idx], ni.Width)
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
	expectedWidths := []int{1, 1, 2, 3, 1}
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
		if ni.Width != expectedWidths[idx] {
			t.Errorf("Expected task %s to be on width %d, but got %d",
				ni.Node.Task.Id(), expectedWidths[idx], ni.Width)
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
	expectedWidths := []int{
		1,
		1, 2, 3,
		1, 2, 3, 4, 5,
		1, 2, 3,
		1,
		1,
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
		if ni.Width != expectedWidths[idx] {
			t.Errorf("Expected task %s to be on width %d, but got %d",
				ni.Node.Task.Id(), expectedWidths[idx], ni.Width)
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
	n := NewNode(nameTask{Name: "Start"})
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

func TestNextTaskLinkedList(t *testing.T) {
	const N = 100
	l1 := linkedList(N)

	// Should be the same as l1, but constructed using NextTask method
	l2 := NewNode(nameTask{Name: "Start"})
	prev := l2
	for i := 0; i < N-1; i++ {
		prev = prev.NextTask(nameTask{Name: fmt.Sprintf("step_%d", i)})
	}

	l1Hash := l1.Hash()
	l2Hash := l2.Hash()

	if l1Hash != l2Hash {
		t.Errorf("Expected same hashes for l1 and l2, got: %s and %s",
			l1Hash, l2Hash)
	}
}

func TestNextTask33Graph(t *testing.T) {
	g1 := deep3Width3Graph()

	g2 := NewNode(constTask{})
	g2.NextTask(aTask{}).NextTask(emptyTask{})
	g2.NextTask(bTask{})
	g2.NextTask(constTask{})

	g1Hash := g1.Hash()
	g2Hash := g2.Hash()

	if g1Hash != g2Hash {
		t.Errorf("Expected the same hashes for g1 and g2, got: %s and %s",
			g1Hash, g2Hash)
	}
}

func TestJointTasksExecSources(t *testing.T) {
	n1 := NewNode(constTask{})
	n2 := NewNode(constTask{})
	n3 := NewNode(constTask{})
	n1.Next(n2).Next(n3)

	execSources := n1.joinTasksExecSources()
	expectedExecSources := `ConstTask:{
	fmt.Println("Executing...")
	return nil
}ConstTask:{
	fmt.Println("Executing...")
	return nil
}ConstTask:{
	fmt.Println("Executing...")
	return nil
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
	return nil
}A:{
	fmt.Println("A")
	return nil
}B:{
	fmt.Println("B")
	return nil
}ConstTask:{
	fmt.Println("Executing...")
	return nil
}EmptyTask:{
	return nil
}`
	if string(execSources) != expectedExecSources {
		t.Errorf("Expected %s, but got %s", expectedExecSources,
			string(execSources))
	}
}

// It's exactly the same as aTask to test hashing
type aTaskCopy struct{}

func (at aTaskCopy) Id() string { return "A" }

func (at aTaskCopy) Execute(_ TaskContext) error {
	fmt.Println("A")
	return nil
}

func TestNodeHashesForSimilarNodes(t *testing.T) {
	n1 := NewNode(aTask{})
	n2 := NewNode(aTaskCopy{})
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

func (at aTaskWithSpace) Id() string { return "A" }

func (at aTaskWithSpace) Execute(_ TaskContext) error {
	fmt.Println("A ")
	return nil
}

func TestNodeAlmostTheSame(t *testing.T) {
	n1 := NewNode(aTask{})
	n2 := NewNode(aTaskWithSpace{})
	h1 := n1.Hash()
	h2 := n2.Hash()

	if h1 == h2 {
		t.Error("Expected different hashes for aTask and aTaskWithSpace but got the same")
	}
}

type aTaskDifferentId struct{}

func (at aTaskDifferentId) Id() string { return "Not A" }

func (at aTaskDifferentId) Execute(_ TaskContext) error {
	fmt.Println("A ")
	return nil
}

func TestNodeTheSameExecute(t *testing.T) {
	n1 := NewNode(aTask{})
	n2 := NewNode(aTaskDifferentId{})
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
	n1 := NewNode(constTask{})
	n21 := NewNode(aTask{})
	n22 := NewNode(bTask{})
	n23 := NewNode(constTask{})
	n3 := NewNode(emptyTask{})
	n1.Next(n21).Next(n3)
	n1.Next(n22)
	n1.Next(n23)

	return n1
}

func nameTaskNode(name string) *Node {
	return NewNode(nameTask{Name: name})
}

func linkedList(length int) *Node {
	s := NewNode(nameTask{Name: "Start"})
	prev := s
	for i := 0; i < length-1; i++ {
		n := NewNode(nameTask{Name: fmt.Sprintf("step_%d", i)})
		prev = prev.Next(n)
	}
	return s
}

func binaryTree(depth int) *Node {
	return balancedTree(depth, 2)
}

func balancedTree(depth int, childNum int) *Node {
	r := NewNode(nameTask{Name: "Node"})
	buildBalancedTree(r, childNum, 1, depth)
	return r
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
		n := NewNode(nameTask{Name: fmt.Sprintf("%s_%d", node.Task.Id(), i)})
		node.Next(n)
	}
}
