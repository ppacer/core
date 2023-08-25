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

// Graph is a valid DAG when the following conditions are met:
//   - Is acyclic (does not have cycles)
//   - Task identifiers are unique within the graph
//   - Graph is no deeper then MAX_RECURSION
func (d *Dag) IsValid() bool {
	return d.Root.isAcyclic() && d.Root.taskIdsUnique() && d.Root.depth() <= MAX_RECURSION
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
}
