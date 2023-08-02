package dag

import (
	"fmt"
	"strings"
)

type Task interface {
	Id() string
	Execute()
	// TODO...
}

type Attr struct {
	Id       string
	Schedule string
}

type Dag struct {
	Attr Attr
	Root *Node
}

func New(attr Attr, root *Node) *Dag {
	return &Dag{
		Attr: attr,
		Root: root,
	}
}

type Node struct {
	Task     Task
	Children []*Node
}

func (dn *Node) Next(node *Node) {
	if dn.Children == nil {
		dn.Children = make([]*Node, 0, 10)
	}
	dn.Children = append(dn.Children, node)
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
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
