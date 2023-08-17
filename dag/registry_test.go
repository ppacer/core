package dag

import "testing"

func TestAddDagToRegistry(t *testing.T) {
	dag := New(Attr{Id: Id("test_dag"), Schedule: "10 * * * *"}, nil)
	Add(dag)
	d2, getErr := Get(Id("test_dag"))
	if getErr != nil {
		t.Errorf("Expected 'test_dag' to exist in the registry got: %s",
			getErr.Error())
	}
	if d2 != dag {
		t.Errorf("Expected DAGs to be the same, got: %v vs %v", dag, d2)
	}
	// clean up
	delete(registry, Id("test_dag"))
}

func TestRegistryListEmpty(t *testing.T) {
	if len(List()) > 0 {
		t.Errorf("Expected empty registry, got: %v", List())
	}
}
