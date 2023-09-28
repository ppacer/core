package user

import (
	"go_shed/src/dag"
	"testing"
)

// Test whenever all dags added in dag.registry are valid DAGs.
func TestDagsValidation(t *testing.T) {
	for _, d := range dag.List() {
		if !d.IsValid() {
			t.Errorf("Dag %s is not a valid DAG!", string(d.Id))
		}
	}
}
