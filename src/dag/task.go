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
	execSources := dn.joinTasksExecSources([]byte{})
	hasher := sha256.New()
	hasher.Write(execSources)
	return hex.EncodeToString(hasher.Sum(nil))
}

// This method is getting DAG tasks Execute() methods source code and join it into single []byte. Traversal is depth
// first search.
func (dn *Node) joinTasksExecSources(data []byte) []byte {
	taskId := []byte(dn.Task.Id() + ":")
	data = append(data, taskId...)
	data = append(data, []byte(TaskExecuteSource(dn.Task))...)
	for _, child := range dn.Children {
		data = child.joinTasksExecSources(data)
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
