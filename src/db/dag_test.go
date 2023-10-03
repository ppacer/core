package db

import (
	"context"
	"database/sql"
	"go_shed/src/version"
	"path"
	"testing"
	"time"
)

var sqlSchemaPath = path.Join("..", "..", "schema.sql")

func TestReadDagFromEmpty(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	_, rErr := c.ReadDag(ctx, "mock_dag")
	if rErr != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, but got: %s", rErr.Error())
	}
}

func TestInsertDagSimple(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	insertSimpleDagAndTest(c, t)
}

func TestInsertDagAndUpdate(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}

	// Insert DAG row for the first time
	firstHashTasks, firstHashMeta := insertSimpleDagAndTest(c, t)

	ctx := context.Background()
	dagId := "my_simple_dag"
	uDag := simpleDag(dagId, 5)
	uErr := c.UpsertDag(ctx, uDag)
	if uErr != nil {
		t.Errorf("Expected no error while updating DAG in dags, got: %s", uErr.Error())
	}
	dbDag, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Errorf("Could not read just updated row from dags table, err: %s", rErr.Error())
	}

	if dbDag.HashTasks == "" {
		t.Error("Expected non-empty hash tasks after row in dags table was updated")
	}
	if firstHashTasks == dbDag.HashTasks {
		t.Errorf("Expected different hash tasks after updating the row. In both cases got: %s", firstHashTasks)
	}
	if firstHashMeta != dbDag.HashDagMeta {
		t.Errorf("Expected the same hash of attributes after updating the row. Got first: %s and after update: %s",
			firstHashMeta, dbDag.HashDagMeta)
	}
	if dbDag.LatestUpdateTs == nil {
		t.Error("Expected non-empty LatestUpdateTs after row in dags table was updated")
	}
	if dbDag.LatestUpdateVersion == nil {
		t.Error("Expected non-empty LatestUpdateVersion after row in dags table was updated")
	}
}

func TestInsertDagTimeout(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}

	ctx, cancle := context.WithTimeout(context.Background(), 10*time.Microsecond)
	defer cancle()

	dagId := "my_simple_dag"
	uDag := simpleDag(dagId, 5)
	uErr := c.UpsertDag(ctx, uDag)
	if uErr == nil {
		t.Errorf("Expected error when context is done due to timeout, but got nil")
	}
}

func insertSimpleDagAndTest(c *Client, t *testing.T) (string, string) {
	ctx := context.Background()
	dagId := "my_simple_dag"
	d := simpleDag(dagId, 1)
	iErr := c.UpsertDag(ctx, d)
	if iErr != nil {
		t.Errorf("Expected no error while inserting DAG into dags, got: %s", iErr.Error())
	}
	dagFromDb, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Errorf("Could not read just inserted row from dags table, err: %s", rErr.Error())
	}
	if dagFromDb.DagId != dagId {
		t.Errorf("Expected DagId=%s dags table, but got: %s", dagId, dagFromDb.DagId)
	}
	if dagFromDb.CreateVersion != version.Version {
		t.Errorf("Expected CreateVersion=%s in dags table, but got: %s", version.Version, dagFromDb.CreateVersion)
	}
	if dagFromDb.LatestUpdateTs != nil {
		t.Errorf("Expected NULL LatestUpdateTs in dags table, got: %s", *dagFromDb.LatestUpdateTs)
	}
	if dagFromDb.LatestUpdateVersion != nil {
		t.Errorf("Expected NULL LatestUpdateVersion in dags table, got: %s", *dagFromDb.LatestUpdateVersion)
	}
	if dagFromDb.HashTasks == "" {
		t.Error("Expected non-empty HashTasks")
	}
	if dagFromDb.HashDagMeta == "" {
		t.Error("Expected non-empty HashDagMeta")
	}
	return dagFromDb.HashTasks, dagFromDb.HashDagMeta
}
