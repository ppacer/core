package dag

import (
	"fmt"
)

type Attr struct {
	Id       Id
	Schedule string
}

type Dag struct {
	Attr Attr
	Root *Node
}

func New(attr Attr, root *Node) Dag {
	return Dag{
		Attr: attr,
		Root: root,
	}
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
}
