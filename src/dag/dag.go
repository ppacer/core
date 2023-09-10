package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

const LOG_PREFIX = "dag"

var ErrTaskNotFoundInDag = errors.New("task was not found in the DAG")

type Attr struct {
	Id       Id     `json:"id"`
	Schedule string `json:"schedule"`
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
	nodesInfo := d.Root.flatten()
	for _, ni := range nodesInfo {
		if ni.Node.Task.Id() == taskId {
			return ni.Node.Task, nil
		}
	}
	return nil, ErrTaskNotFoundInDag
}

// Flatten DAG into list of Tasks in BFS order.
func (d *Dag) Flatten() []Task {
	if d.Root == nil {
		return []Task{}
	}
	nodesInfo := d.Root.flatten()
	tasks := make([]Task, len(nodesInfo))
	for idx, ni := range nodesInfo {
		tasks[idx] = ni.Node.Task
	}
	return tasks
}

// HashAttr calculates SHA256 hash based on DAG attribues.
func (d *Dag) HashAttr() string {
	attrJson, jErr := json.Marshal(d.Attr)
	if jErr != nil {
		log.Error().Err(jErr).Msgf("[%s] Cannot serialize DAG attributes [%v]", LOG_PREFIX, d.Attr)
		return "CANNOT SERIALIZE DAG ATTRIBUTES"
	}
	hasher := sha256.New()
	hasher.Write(attrJson)
	return hex.EncodeToString(hasher.Sum(nil))
}

// HashTasks calculates SHA256 hash based on concatanated body sources of Execute methods for all tasks.
func (d *Dag) HashTasks() string {
	if d.Root == nil {
		hasher := sha256.New()
		hasher.Write([]byte("NO TASKS"))
		return hex.EncodeToString(hasher.Sum(nil))
	}
	return d.Root.Hash()
}

func (d *Dag) String() string {
	return fmt.Sprintf("Dag: %s (%s)\nTasks:\n%s", d.Attr.Id, d.Attr.Schedule, d.Root.String(0))
}
