package db

import (
	"database/sql"
	"go_shed/src/version"
	"testing"
)

func TestReadDagFromEmpty(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}
	_, rErr := c.ReadDag("mock_dag")
	if rErr != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, but got: %s", rErr.Error())
	}
}

func TestInsertDagSimple(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}
	insertSimpleDagAndTest(c, t)
}

func TestInsertDagAndUpdate(t *testing.T) {
	c, err := emptyDbWithSchema()
	if err != nil {
		t.Error(err)
	}

	// Insert DAG row for the first time
	firstHash := insertSimpleDagAndTest(c, t)

	dagId := "my_simple_dag"
	uDag := simpleDag(dagId, 5)
	uErr := c.UpsertDag(uDag)
	if uErr != nil {
		t.Errorf("Expected no error while updating DAG in dags, got: %s", uErr.Error())
	}
	dbDag, rErr := c.ReadDag(dagId)
	if rErr != nil {
		t.Errorf("Could not read just updated row from dags table, err: %s", rErr.Error())
	}

	if dbDag.Hash == "" {
		t.Error("Expected non-empty hash after row in dags table was updated")
	}
	if firstHash == dbDag.Hash {
		t.Errorf("Expected different hash after updating the row. In both cases got: %s", firstHash)
	}
	if dbDag.LatestUpdateTs == nil {
		t.Error("Expected non-empty LatestUpdateTs after row in dags table was updated")
	}
	if dbDag.LatestUpdateVersion == nil {
		t.Error("Expected non-empty LatestUpdateVersion after row in dags table was updated")
	}
}

func insertSimpleDagAndTest(c *Client, t *testing.T) string {
	dagId := "my_simple_dag"
	d := simpleDag(dagId, 1)
	iErr := c.UpsertDag(d)
	if iErr != nil {
		t.Errorf("Expected no error while inserting DAG into dags, got: %s", iErr.Error())
	}
	dagFromDb, rErr := c.ReadDag(dagId)
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
	if dagFromDb.Hash == "" {
		t.Error("Expected non-empty Hash")
	}
	return dagFromDb.Hash
}
