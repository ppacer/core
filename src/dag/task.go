package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go_shed/src/meta"
	"reflect"
	"strings"

	"github.com/rs/zerolog/log"
)

const MAX_RECURSION = 10000

// Task represents single step in DAG which is going to be scheduled and executed via executors.
type Task interface {
	Id() string
	Execute()
}

// TaskExecuteSource returns Task's source code of its Execute() method. In case when method source code cannot be
// found in the AST (meta.PackagesASTsMap) string with message "NO IMPLEMENTATION FOUND..." would be returned. Though
// it should be the case only when whole new package is not added to the embedding (src/embed.go).
func TaskExecuteSource(t Task) string {
	tTypeName := reflect.TypeOf(t).Name()
	_, execMethodSource, err := meta.MethodBodySource(meta.PackagesASTsMap, tTypeName, "Execute")
	if err != nil {
		log.Error().Err(err).Msgf("Could not get %s.Execute() source code", tTypeName)
		return fmt.Sprintf("NO IMPLEMENTATION FOUND FOR %s.Execute()", tTypeName)
	}
	return execMethodSource
}

// Node represents single node (vertex) in the DAG.
type Node struct {
	Task     Task
	Children []*Node
}

// TODO(ds): docs
func (dn *Node) Next(node *Node) {
	if dn.Children == nil {
		dn.Children = make([]*Node, 0, 10)
	}
	dn.Children = append(dn.Children, node)
}

// TODO(ds): docs
func (dn *Node) Hash() string {
	execSources := dn.joinTasksExecSources()
	hasher := sha256.New()
	hasher.Write(execSources)
	return hex.EncodeToString(hasher.Sum(nil))
}

// Get graph depth. Single node has depth=1.
func (dn *Node) depth() int {
	maxChildDepth := 0
	for _, child := range dn.Children {
		childDepth := child.depth()
		if childDepth > maxChildDepth {
			maxChildDepth = childDepth
		}
	}
	return maxChildDepth + 1
}

// Checks whenever graph starting from this node does not have cycles.
func (dn *Node) isAcyclic() bool {
	nodeMap := make(map[*Node]struct{})
	return dn.isAcyclicImpl(nodeMap, 0)
}

// Checks whenever address of a node already exists in the set of traversed nodes, to determine cycles. If traversing
// depth exceeds MAX_RECURSION, then false is returned and further examination is stopped.
func (dn *Node) isAcyclicImpl(traversed map[*Node]struct{}, depth int) bool {
	if depth > MAX_RECURSION {
		log.Error().Msgf("Max recursion depth reached (%d). Cannot determine if grapth is acyclic.",
			MAX_RECURSION)
		return false
	}
	if _, alreadyTraversed := traversed[dn]; alreadyTraversed {
		return false
	}
	traversed[dn] = struct{}{}
	for _, child := range dn.Children {
		check := child.isAcyclicImpl(traversed, depth+1)
		if !check {
			return false
		}
	}
	return true
}

// Flattens tasks into a list of tasks. It can be done either bread-first-search (BFS) or deep-first-search (DFS)
// order. List of tasks might be incomplete if depth of the graph exceeds MAX_RECURSION value.
func (dn *Node) flatten(bfs bool) []Task {
	if bfs {
		return dn.flattenBFS()
	}
	tasks := make([]Task, 0, 100)
	return dn.flattenDFS(tasks, 0)
}

func (dn *Node) flattenBFS() []Task {
	var ts []Task
	var queue []*Node
	depthMarker := &Node{}
	depth := 0
	queue = append(queue, dn, depthMarker)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == depthMarker {
			depth++
			if depth > MAX_RECURSION {
				log.Error().Msgf("Max level reached (%d). Returned result might be incomplete.", MAX_RECURSION)
				break
			}
			if len(queue) > 0 {
				queue = append(queue, depthMarker)
			}
			continue
		}
		ts = append(ts, current.Task)
		queue = append(queue, current.Children...)
	}
	return ts
}

func (dn *Node) flattenDFS(ts []Task, depth int) []Task {
	if depth > MAX_RECURSION {
		log.Error().Msgf("Max recursion depth reached (%d). Returned result might be incomplete.",
			MAX_RECURSION)
		return ts
	}
	ts = append(ts, dn.Task)
	for _, child := range dn.Children {
		ts = child.flattenDFS(ts, depth+1)
	}
	return ts
}

func (dn *Node) taskIdsUnique() bool {
	tasks := dn.flatten(true)
	taskIds := make(map[string]struct{})

	for _, task := range tasks {
		if _, alreadyExists := taskIds[task.Id()]; alreadyExists {
			return false
		}
		taskIds[task.Id()] = struct{}{}
	}
	return true
}

// This method is getting DAG tasks Execute() methods source code and join it into single []byte. Traversal is in BFS
// order.
func (dn *Node) joinTasksExecSources() []byte {
	data := make([]byte, 0, 1024)
	tasks := dn.flatten(true)
	for _, task := range tasks {
		taskId := []byte(task.Id() + ":")
		data = append(data, taskId...)
		data = append(data, []byte(TaskExecuteSource(task))...)
	}
	return data
}

func (n *Node) String(ident int) string {
	var s strings.Builder
	return n.stringRec(&s, ident)
}

func (n *Node) stringRec(s *strings.Builder, depth int) string {
	const indent = 2
	if depth > 0 {
		depth++
	}
	addSpaces(s, depth*indent)
	s.WriteString(fmt.Sprintf("-%s\n", n.Task.Id()))
	for _, ch := range n.Children {
		ch.stringRec(s, depth+1)
	}
	return s.String()
}

// TODO: Perhaps move to another package?
func addSpaces(builder *strings.Builder, n int) *strings.Builder {
	for i := 0; i < n; i++ {
		builder.WriteString(" ")
	}
	return builder
}
