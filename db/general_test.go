package db

import (
	"context"
	"testing"
)

func TestGroupBy2EmptyTable(t *testing.T) {
	c, err := NewSqliteInMemoryClient(testLogger())
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	query := "SELECT Status, COUNT(*) FROM dagruns GROUP BY Status"
	res, qErr := groupBy2[string, int](ctx, c.dbConn, c.logger, query)
	if qErr != nil {
		t.Errorf("Error while groupBy2 on empty table: %s", qErr.Error())
	}
	if len(res) != 0 {
		t.Errorf("Expected 0 results, got: %d", len(res))
	}
}

func TestGroupBy2StringInt(t *testing.T) {
	c, err := NewSqliteInMemoryClient(testLogger())
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	dagId := "mock_dag"
	timestamps := []string{
		"2023-09-23T10:10:00",
		"2023-09-23T10:20:00",
		"2023-09-23T10:30:00",
		"2023-09-23T10:40:00",
		"2023-09-23T10:50:00",
		"2023-09-23T11:00:00",
	}
	for _, ts := range timestamps {
		insertDagRun(c, ctx, dagId, ts, t)
	}

	query := "SELECT Status, COUNT(*) FROM dagruns GROUP BY Status"
	res, qErr := groupBy2[string, int](ctx, c.dbConn, c.logger, query)
	if qErr != nil {
		t.Errorf("Error while groupBy2: %s", qErr.Error())
	}
	if len(res) != 1 {
		t.Errorf("Expected 1 result, got: %d", len(res))
	}
	dagruns, exists := res[DagRunTaskStatusScheduled]
	if !exists {
		t.Errorf("Expected key=%s to exist in aggregated results, but it does not",
			DagRunTaskStatusScheduled)
	}
	if dagruns != len(timestamps) {
		t.Errorf("Expectes %d DAG runs in status %s, but got: %d",
			len(timestamps), DagRunTaskStatusScheduled, dagruns)
	}
}

func TestGroupBy2StringString(t *testing.T) {
	c, err := NewSqliteInMemoryClient(testLogger())
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	dagId := "mock_dag"
	timestamps := []string{
		"2023-09-23T10:10:00",
		"2023-09-23T10:20:00",
		"2023-09-23T10:30:00",
	}
	for _, ts := range timestamps {
		insertDagRun(c, ctx, dagId, ts, t)
	}

	query := "SELECT Status, MAX(DagId) FROM dagruns GROUP BY Status"
	res, qErr := groupBy2[string, string](ctx, c.dbConn, c.logger, query)
	if qErr != nil {
		t.Errorf("Error while groupBy2: %s", qErr.Error())
	}
	if len(res) != 1 {
		t.Errorf("Expected 1 result, got: %d", len(res))
	}
	resDagId, exists := res[DagRunTaskStatusScheduled]
	if !exists {
		t.Errorf("Expected key=%s to exist in aggregated results, but it does not",
			DagRunTaskStatusScheduled)
	}
	if resDagId != dagId {
		t.Errorf("Expected result dagId=%s for status %s, but got: %s",
			dagId, DagRunTaskStatusScheduled, resDagId)
	}
}
