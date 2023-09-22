package db

import (
	"testing"
	"time"
)

func TestInsertDagRunSimple(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	dagId := "mock_dag"
	execTs := time.Now().Format(InsertTsFormat)
	runId, iErr := c.InsertDagRun(dagId, execTs)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
	if runId != 1 {
		t.Errorf("Expected RunId=1, got: %d", runId)
	}

	c1 := c.Count("dagruns")
	if c1 != 1 {
		t.Errorf("Expected 1 row got: %d", c1)
	}

	execTs = time.Now().Format(InsertTsFormat)
	runId, iErr = c.InsertDagRun(dagId, execTs)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
	if runId != 2 {
		t.Errorf("Expected RunId=2, got %d", runId)
	}
	c2 := c.Count("dagruns")
	if c2 != 2 {
		t.Errorf("Expected 2 row got: %d", c2)
	}
}
