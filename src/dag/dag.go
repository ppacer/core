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

// TODO(dskrzypiec): docs
func (d *Dag) IsValid() bool {
	if !d.Root.isAcyclic() {
		return false
	}
	return true
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
}
