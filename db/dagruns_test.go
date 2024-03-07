// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestInsertDagRunSimple(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag"
	execTs := timeutils.ToString(time.Now())
	runId, iErr := c.InsertDagRun(ctx, dagId, execTs)
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

	execTs = timeutils.ToString(time.Now())
	runId, iErr = c.InsertDagRun(ctx, dagId, execTs)
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
	c, err := NewSqliteInMemoryClient(nil)
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

	// Read all dag runs for mock_dag
	dagRunsAll, rErr := c.ReadDagRuns(ctx, dagId, -1)
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
		if dr.Status != statusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", statusScheduled, dr.Status)
		}
	}
}

func TestInsertAndReadDagRunsTop3(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
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

	// Read dag runs for top 3 runs
	const topN = 3
	dagRunsAll, rErr := c.ReadDagRuns(ctx, dagId, topN)
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
		if dr.Status != statusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", statusScheduled, dr.Status)
		}
	}
}

func TestInsertAndReadDagRunsTop1000(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
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

	// Read all dag runs for mock_dag
	const topN = 1000
	dagRunsAll, rErr := c.ReadDagRuns(ctx, dagId, topN)
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
		if dr.Status != statusScheduled {
			t.Errorf("Expected Status=%s, but got: %s", statusScheduled, dr.Status)
		}
	}
}

func TestInsertAndReadDagRunsTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		// TODO: analyze why this runs differently on Windows.
		t.Skip("This test behaves differently on Windows, than on Linux and MacOS")
		return
	}
	c, err := NewSqliteInMemoryClient(nil)
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

	// Read all dag runs for mock_dag
	const topN = 1000
	ctx, cancel := context.WithTimeout(ctx, 10*time.Nanosecond)
	defer cancel()
	dagRunsAll, rErr := c.ReadDagRuns(ctx, dagId, topN)
	if rErr == nil {
		t.Error("Expected non-nil error for ReadDagRuns with Timeout in nanoseconds")
	}
	if dagRunsAll != nil {
		t.Errorf("Expected nil ReadDagRuns result, got: %v", dagRunsAll)
	}
}

func TestReadLatestDagRunsSimple(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId1 := "mock_dag_1"
	timestamps1 := []string{
		"2023-09-23T10:10:00",
		"2023-09-23T10:20:00",
		"2023-09-23T10:30:00",
		"2023-09-23T10:40:00",
		"2023-09-23T10:50:00",
		"2023-09-23T11:00:00",
	}
	dagId2 := "mock_dag_2"
	timestamp2 := "2023-09-23T10:10:00"

	// Insert dagruns for mock_dag_1
	for _, ts := range timestamps1 {
		insertDagRun(c, ctx, dagId1, ts, t)
	}

	// Insert dagrun for mock_dag_2
	insertDagRun(c, ctx, dagId2, timestamp2, t)
	dag1Count := c.CountWhere("dagruns", "DagId='mock_dag_1'")
	if dag1Count != 6 {
		t.Errorf("Expected 6 dag runs for %s, got %d", dagId1, dag1Count)
	}
	dag2Count := c.CountWhere("dagruns", "DagId='mock_dag_2'")
	if dag2Count != 1 {
		t.Errorf("Expected 1 dag run for %s, got %d", dagId2, dag2Count)
	}

	latestDagRuns, lErr := c.ReadLatestDagRuns(ctx)
	if lErr != nil {
		t.Fatalf("Error while reading latest dag runs: %s", lErr.Error())
	}
	if len(latestDagRuns) != 2 {
		t.Errorf("Expected latest dag runs for 2 dags, got %d", len(latestDagRuns))
	}
	for _, ldr := range latestDagRuns {
		if ldr.DagId == dagId1 && ldr.ExecTs != timestamps1[5] {
			t.Errorf("Expected latest dag run for %s to be %s, got: %s", dagId1, timestamps1[5], ldr.ExecTs)
		}
		if ldr.DagId == dagId2 && ldr.ExecTs != timestamp2 {
			t.Errorf("Expected latest dag run for %s to be %s, got: %s", dagId2, timestamp2, ldr.ExecTs)
		}
		if ldr.DagId == dagId1 && ldr.RunId != 6 {
			t.Errorf("Expected latest dag run for %s to have runId=%d, got: %d", dagId1, 6, ldr.RunId)
		}
		if ldr.DagId == dagId2 && ldr.RunId != 7 {
			t.Errorf("Expected latest dag run for %s to have runId=%d, got: %d", dagId2, 1, ldr.RunId)
		}
	}
}

func TestDagRunUpdateStatus(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag_1"
	timestamp := "2023-09-23T10:10:00"
	insertDagRun(c, ctx, dagId, timestamp, t)

	cnt := c.Count("dagruns")
	if cnt != 1 {
		t.Errorf("Expected 1 dag runs for %s, got %d", dagId, cnt)
	}
	dagruns, err := c.ReadDagRuns(ctx, dagId, 1)
	if err != nil {
		t.Fatalf("Error while reading dagruns for DagId=%s: %s", dagId, err.Error())
	}

	dr := dagruns[0]
	t1, tErr := timeutils.FromString(dr.StatusUpdateTs)
	if tErr != nil {
		t.Errorf("Cannot convert to time.Time from %s", dr.StatusUpdateTs)
	}
	if dr.Status != statusScheduled {
		t.Errorf("Expected status %s for the dag run before update, got: %s", statusScheduled, dr.Status)
	}

	time.Sleep(1 * time.Millisecond)
	const updateStatus1 = "TEST_STATUS_1"
	uErr := c.UpdateDagRunStatus(ctx, dr.RunId, updateStatus1)
	if uErr != nil {
		t.Fatalf("Error while updating dagrun for RunId=%d: %s", dr.RunId, uErr.Error())
	}
	dagruns2, err2 := c.ReadDagRuns(ctx, dagId, 1)
	if err2 != nil {
		t.Fatalf("Error while reading dagruns for DagId=%s: %s", dagId, err2.Error())
	}
	dr = dagruns2[0]
	if dr.Status != updateStatus1 {
		t.Errorf("Expected status %s after the update, but got: %s", updateStatus1, dr.Status)
	}
	t2, tErr2 := timeutils.FromString(dr.StatusUpdateTs)
	if tErr2 != nil {
		t.Errorf("Cannot convert to time.Time from %s after the update", dr.StatusUpdateTs)
	}
	if t1.Compare(t2) != -1 {
		t.Errorf("Expecte to be %v earlier than %v", t1, t2)
	}
}

func TestDagRunUpdateStatusByExecTs(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag_1"
	timestamp := timeutils.ToString(time.Date(2023, 10, 5, 12, 0, 0, 0, time.UTC))
	insertDagRun(c, ctx, dagId, timestamp, t)

	cnt := c.Count("dagruns")
	if cnt != 1 {
		t.Errorf("Expected 1 dag runs for %s, got %d", dagId, cnt)
	}
	dagruns, err := c.ReadDagRuns(ctx, dagId, 1)
	if err != nil {
		t.Fatalf("Error while reading dagruns for DagId=%s: %s", dagId, err.Error())
	}

	dr := dagruns[0]
	t1, tErr := timeutils.FromString(dr.StatusUpdateTs)
	if tErr != nil {
		t.Errorf("Cannot convert to time.Time from %s", dr.StatusUpdateTs)
	}
	if dr.Status != statusScheduled {
		t.Errorf("Expected status %s for the dag run before update, got: %s", statusScheduled, dr.Status)
	}

	time.Sleep(1 * time.Millisecond)
	const updateStatus1 = "TEST_STATUS_1"
	uErr := c.UpdateDagRunStatusByExecTs(ctx, dagId, timestamp, updateStatus1)
	if uErr != nil {
		t.Fatalf("Error while updating dagrun for DagId=%s and ExecTs=%s: %s", dagId, timestamp, uErr.Error())
	}
	dagruns2, err2 := c.ReadDagRuns(ctx, dagId, 1)
	if err2 != nil {
		t.Fatalf("Error while reading dagruns for DagId=%s: %s", dagId, err2.Error())
	}
	dr = dagruns2[0]
	if dr.Status != updateStatus1 {
		t.Errorf("Expected status %s after the update, but got: %s", updateStatus1, dr.Status)
	}
	t2, tErr2 := timeutils.FromString(dr.StatusUpdateTs)
	if tErr2 != nil {
		t.Errorf("Cannot convert to time.Time from %s after the update", dr.StatusUpdateTs)
	}
	if t1.Compare(t2) != -1 {
		t.Errorf("Expecte to be %v earlier than %v", t1, t2)
	}
}

func TestDagRunUpdateStatusNoRun(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	// There is no dagruns rows at all at this point
	ctx := context.Background()
	const status = "TEST_STATUS"
	uErr := c.UpdateDagRunStatus(ctx, 1234, status)
	if uErr == nil {
		t.Error("Expected non-empty error while updating dag run state for non existing runId")
	}
}

func TestDagRunUpdateStatusByExecTsNoRun(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	// There is no dagruns rows at all at this point
	ctx := context.Background()
	const status = "TEST_STATUS"
	uErr := c.UpdateDagRunStatusByExecTs(ctx, "test_dag", timeutils.ToString(time.Now()), status)
	if uErr == nil {
		t.Error("Expected non-empty error while updating dag run state for non existing runId")
	}
}

func TestDagRunExistsOnEmpty(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag_1"
	timestamp := "2023-09-23T10:10:00"

	exists, err := c.DagRunAlreadyScheduled(ctx, dagId, timestamp)
	if err != nil {
		t.Errorf("Expected non-nil error, got: %s", err.Error())
	}
	if exists {
		t.Error("DagRunExists on empty table should return false")
	}
}

func TestDagRunExistsSimple(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
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

	for _, ts := range timestamps {
		exists, err := c.DagRunAlreadyScheduled(ctx, dagId, ts)
		if err != nil {
			t.Errorf("Expected non-nil error, got: %s", err.Error())
		}
		if !exists {
			t.Error("DagRunExists does not exist but should")
		}
	}
}

func TestDagRunsNotFinishedSimple(t *testing.T) {
	c, err := NewSqliteInMemoryClient(nil)
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
	for idx, ts := range timestamps {
		insertDagRun(c, ctx, dagId, ts, t)
		if idx != 2 {
			c.UpdateDagRunStatus(ctx, int64(idx+1), statusSuccess)
		}
	}

	dagRunsToBeScheduled, err := c.ReadDagRunsNotFinished(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(dagRunsToBeScheduled) != 1 {
		t.Errorf("Expected 1 dag run to be scheduled, got: %d", len(dagRunsToBeScheduled))
	}
	dr := dagRunsToBeScheduled[0]
	if dr.RunId != 3 {
		t.Errorf("Expected DAG with SCHEDULED status to have RunId=3, got: %d", dr.RunId)
	}
}

func TestDagRunsNotFinishedForTerminalStates(t *testing.T) {
	c, err := NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	const dagId = "mock_dag"
	const N = 10
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < N; i++ {
		ts := timeutils.ToString(t0.Add(time.Duration(i) * time.Hour))
		insertDagRun(c, ctx, dagId, ts, t)
		status := statusFailed
		if i%2 == 0 {
			status = statusSuccess
		}
		uErr := c.UpdateDagRunStatus(ctx, int64(i+1), status)
		if uErr != nil {
			t.Errorf("Cannot update DAG run status: %s", uErr.Error())
		}
	}

	dagRunsToBeScheduled, err := c.ReadDagRunsNotFinished(ctx)
	if err != nil {
		t.Errorf("Cannot load not finished DAG runs: %s", err.Error())
	}
	if len(dagRunsToBeScheduled) != 0 {
		t.Errorf("Expected 0 DAG runs that are not finished, got: %d",
			len(dagRunsToBeScheduled))
	}
}

func TestDagRunsNotFinishedForRunningStates(t *testing.T) {
	c, err := NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	const dagId = "mock_dag"
	const N = 10
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < N; i++ {
		ts := timeutils.ToString(t0.Add(time.Duration(i) * time.Hour))
		insertDagRun(c, ctx, dagId, ts, t)
		status := "RUNNING"
		uErr := c.UpdateDagRunStatus(ctx, int64(i+1), status)
		if uErr != nil {
			t.Errorf("Cannot update DAG run status: %s", uErr.Error())
		}
	}

	dagRunsToBeScheduled, err := c.ReadDagRunsNotFinished(ctx)
	if err != nil {
		t.Errorf("Cannot load not finished DAG runs: %s", err.Error())
	}
	if len(dagRunsToBeScheduled) != N {
		t.Errorf("Expected %d DAG runs that are not finished, got: %d",
			N, len(dagRunsToBeScheduled))
	}
}

func insertDagRun(c *Client, ctx context.Context, dagId, execTs string, t *testing.T) {
	_, iErr := c.InsertDagRun(ctx, dagId, execTs)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
}
