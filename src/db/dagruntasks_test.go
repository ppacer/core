package db

import (
	"context"
	"testing"
)

func TestReadDagRunTasksFromEmpty(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	drts, err := c.ReadDagRunTasks(ctx, "any_dag", "any_time")
	if err != nil {
		t.Errorf("Expected non-nil error, got: %s", err.Error())
	}
	if len(drts) != 0 {
		t.Errorf("Expected 0 loaded DagRunTasks, got: %d", len(drts))
	}
}
