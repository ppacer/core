package dag

import (
	"fmt"
)

// Registry is a package-level map storing all defined and added DAGs. This registry is used both by scheduler and
// executors.
var registry map[Id]Dag = map[Id]Dag{}

// DAG string identifier.
type Id string

// Add adds new DAG to the registry. If dag is already added in the registry, which means dag.Attr.Id is already a key
// in the registry map, then non-nil error is returned.
func Add(dag Dag) error {
	if _, exists := registry[dag.Id]; exists {
		return fmt.Errorf("Dag %s is already registered", dag.Id)
	}
	registry[dag.Id] = dag
	return nil
}

// Get gets a DAG by its identifier. If given identifier is no in the registry, then non-nil error will be returned.
func Get(dagId Id) (Dag, error) {
	if _, exists := registry[dagId]; !exists {
		return Dag{}, fmt.Errorf("Dag %s is not in the registry", dagId)
	}
	return registry[dagId], nil
}

// List lists all DAGs in the registry.
func List() []Id {
	dagIds := make([]Id, 0, len(registry))
	for id := range registry {
		dagIds = append(dagIds, id)
	}
	return dagIds
}
