package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/dskrzypiec/scheduler/src/meta"
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

// TaskHash returns SHA256 of given Task Execute method body source.
func TaskHash(t Task) string {
	taskBodySource := TaskExecuteSource(t)
	hasher := sha256.New()
	hasher.Write([]byte(taskBodySource))
	return hex.EncodeToString(hasher.Sum(nil))
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
//
//	      an[0]
//	    /       \
//	   /  an[1]  \
//	  / /      \  \
//	dn -- an[2] ---- mergeNode
//	  \ \      /  /
//	   \  an[3]  /
//	    \  ...  /
//	      an[N]
func (dn *Node) NextAsyncAndMerge(asyncNodes []*Node, mergeNode *Node) {
	for _, an := range asyncNodes {
		dn.Next(an)
		an.Next(mergeNode)
	}
}

// Hash calculates SHA256 hash based on concatenated body sources of Execute methods of all children recursively.
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
	nodeMap := make(map[*Node]int)
	return dn.isAcyclicImpl(nodeMap, 0)
}

// Checks whenever address of a node already exists in the set of traversed nodes, to determine cycles. If traversing
// depth exceeds MAX_RECURSION, then false is returned and further examination is stopped.
func (dn *Node) isAcyclicImpl(traversed map[*Node]int, depth int) bool {
	if depth > MAX_RECURSION {
		log.Error().Msgf("Max recursion depth reached (%d). Cannot determine if grapth is acyclic.",
			MAX_RECURSION)
		return false
	}
	// condition for traversedOnDepth < depth-1 is for case when there are multiply nodes merging into one node
	if traversedOnDepth, alreadyTraversed := traversed[dn]; alreadyTraversed && traversedOnDepth < depth-1 {
		return false
	}
	traversed[dn] = depth
	for _, child := range dn.Children {
		check := child.isAcyclicImpl(traversed, depth+1)
		if !check {
			return false
		}
	}
	return true
}

// NodeInfo represents enriched information about node in the DAG. It's used mostly for convenience. In particular it's
// used to flatten DAG into slice of NodeInfo.
type NodeInfo struct {
	Node    *Node
	Depth   int
	Parents []*Node
}

// Flattens tree (DAG) into a list of NodeInfo. Flattening is done in BFS order. Result slice does not contain duplicates.
// List of nodes might be incomplete if depth of the graph exceeds MAX_RECURSION value.
func (dn *Node) flatten() []NodeInfo {
	flattenNodes, parentsMap := dn.flattenBFS()
	ni := make([]NodeInfo, 0, len(flattenNodes))
	for idx, nodeD := range flattenNodes {
		thereIsBetterCandidate := false
		if idx < len(flattenNodes)-1 {
			for i := idx + 1; i < len(flattenNodes); i++ {
				if flattenNodes[i].Node == nodeD.Node {
					// This is for case where there are several edges from different depth level of graph into the same
					// target node. In such case we want to put target node on level of deepest edge.
					thereIsBetterCandidate = true
					break
				}
			}
		}
		if !thereIsBetterCandidate {
			parents, parentsExists := parentsMap[nodeD.Node]
			if parentsExists {
				nodeD.Parents = parents
			}
			ni = append(ni, nodeD)
		}
	}
	return ni
}

func (dn *Node) flattenBFS() ([]NodeInfo, map[*Node][]*Node) {
	visited := make(map[*Node]int)
	parentsMap := make(map[*Node][]*Node)
	var ni []NodeInfo
	var queue []*Node
	depthMarker := &Node{}
	depth := 1
	queue = append(queue, dn, depthMarker)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visitedOnDepth, alreadyVisited := visited[current]; alreadyVisited && visitedOnDepth == depth {
			continue
		}
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
		visited[current] = depth
		ni = append(ni, NodeInfo{Node: current, Depth: depth, Parents: nil})
		for _, child := range current.Children {
			parentsMap[child] = append(parentsMap[child], current)
			queue = append(queue, child)
		}
	}
	return ni, parentsMap
}

func (dn *Node) taskIdsUnique() bool {
	nodesInfo := dn.flatten()
	taskIds := make(map[string]struct{})

	for _, ni := range nodesInfo {
		if _, alreadyExists := taskIds[ni.Node.Task.Id()]; alreadyExists {
			return false
		}
		taskIds[ni.Node.Task.Id()] = struct{}{}
	}
	return true
}

// This method is getting DAG tasks Execute() methods source code and join it into single []byte. Traversal is in BFS
// order.
func (dn *Node) joinTasksExecSources() []byte {
	data := make([]byte, 0, 1024)
	nodesInfo := dn.flatten()
	for _, ni := range nodesInfo {
		taskId := []byte(ni.Node.Task.Id() + ":")
		data = append(data, taskId...)
		data = append(data, []byte(TaskExecuteSource(ni.Node.Task))...)
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
