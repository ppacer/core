package dag

import (
	"errors"
	"fmt"
)

var ErrTaskNotFoundInDag = errors.New("task was not found in the DAG")

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

// GetTask return task by its identifier. In case when there is no Task within the DAG of given taskId, then non-nil
// error will be returned (ErrTaskNotFoundInDag).
func (d *Dag) GetTask(taskId string) (Task, error) {
	tasks := d.Root.flatten(true)
	for _, task := range tasks {
		if task.Id() == taskId {
			return task, nil
		}
	}
	return nil, ErrTaskNotFoundInDag
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
}
