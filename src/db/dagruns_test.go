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

func TestInsertAndReadDagRunsAll(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
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
		insertDagRun(c, dagId, ts, t)
	}

	// Read all dag runs for mock_dag
	dagRunsAll, rErr := c.ReadDagRuns(dagId, -1)
	if rErr != nil {
		t.Fatalf("Error while reading all dag runs for %s: %s", dagId, rErr.Error())
	}

	if len(dagRunsAll) != len(timestamps) {
		t.Errorf("Expected %d dag runs, got %d", len(timestamps), len(dagRunsAll))
		for _, dr := range dagRunsAll {
			t.Logf("%+v", dr)
		}
	}

	rows := len(dagRunsAll)
	for idx, dr := range dagRunsAll {
		if dr.RunId != int64(rows-idx) {
			t.Errorf("Expected RunId=%d, got %d", rows-idx, dr.RunId)
		}
		if dr.ExecTs != timestamps[rows-idx-1] {
			t.Errorf("Expected ExecTs=%s, got: %s", timestamps[rows-idx-1], dr.ExecTs)
		}
		if dr.Status != DagRunStatusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", DagRunStatusScheduled, dr.Status)
		}
	}
}

func TestInsertAndReadDagRunsTop3(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
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
		insertDagRun(c, dagId, ts, t)
	}

	// Read dag runs for top 3 runs
	const topN = 3
	dagRunsAll, rErr := c.ReadDagRuns(dagId, topN)
	if rErr != nil {
		t.Fatalf("Error while reading all dag runs for %s: %s", dagId, rErr.Error())
	}

	if len(dagRunsAll) != topN {
		t.Errorf("Expected %d dag runs, got %d", topN, len(dagRunsAll))
		for _, dr := range dagRunsAll {
			t.Logf("%+v", dr)
		}
	}

	rows := len(timestamps)
	for idx, dr := range dagRunsAll {
		if dr.RunId != int64(rows-idx) {
			t.Errorf("Expected RunId=%d, got %d", rows-idx, dr.RunId)
		}
		if dr.ExecTs != timestamps[rows-idx-1] {
			t.Errorf("Expected ExecTs=%s, got: %s", timestamps[rows-idx-1], dr.ExecTs)
		}
		if dr.Status != DagRunStatusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", DagRunStatusScheduled, dr.Status)
		}
	}
}

func TestInsertAndReadDagRunsTop1000(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
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
		insertDagRun(c, dagId, ts, t)
	}

	// Read all dag runs for mock_dag
	const topN = 1000
	dagRunsAll, rErr := c.ReadDagRuns(dagId, topN)
	if rErr != nil {
		t.Fatalf("Error while reading all dag runs for %s: %s", dagId, rErr.Error())
	}

	if len(dagRunsAll) != len(timestamps) {
		t.Errorf("Expected %d dag runs, got %d", len(timestamps), len(dagRunsAll))
		for _, dr := range dagRunsAll {
			t.Logf("%+v", dr)
		}
	}

	rows := len(dagRunsAll)
	for idx, dr := range dagRunsAll {
		if dr.RunId != int64(rows-idx) {
			t.Errorf("Expected RunId=%d, got %d", rows-idx, dr.RunId)
		}
		if dr.ExecTs != timestamps[rows-idx-1] {
			t.Errorf("Expected ExecTs=%s, got: %s", timestamps[rows-idx-1], dr.ExecTs)
		}
		if dr.Status != DagRunStatusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", DagRunStatusScheduled, dr.Status)
		}
	}
}

func insertDagRun(c *Client, dagId, execTs string, t *testing.T) {
	_, iErr := c.InsertDagRun(dagId, execTs)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
}
