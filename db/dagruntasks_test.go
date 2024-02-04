// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/timeutils"
)

func TestInsertDagRunTaskSimple(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag"
	execTs := timeutils.ToString(time.Now())
	taskId := "my_task_1"
	iErr := c.InsertDagRunTask(
		ctx, dagId, execTs, taskId, DagRunTaskStatusScheduled,
	)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}

	c1 := c.Count("dagruntasks")
	if c1 != 1 {
		t.Errorf("Expected 1 row got: %d", c1)
	}

	taskId2 := "my_task_2"
	iErr2 := c.InsertDagRunTask(
		ctx, dagId, execTs, taskId2, DagRunTaskStatusScheduled,
	)
	if iErr2 != nil {
		t.Errorf("Error while inserting dag run: %s", iErr2.Error())
	}

	c2 := c.Count("dagruntasks")
	if c2 != 2 {
		t.Errorf("Expected 2 row got: %d", c2)
	}

}

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

func TestReadDagRunTasks(t *testing.T) {
	const N = 100
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	dagId := "mock_dag"
	execTs := timeutils.ToString(time.Now())

	for i := 0; i < N; i++ {
		taskId := fmt.Sprintf("my_task_%d", i)
		insertDagRunTask(c, ctx, dagId, execTs, taskId, t)
	}

	dagTasks, rErr := c.ReadDagRunTasks(ctx, dagId, execTs)
	if rErr != nil {
		t.Errorf("Unexpected error while reading dag run tasks: %s",
			rErr.Error())
	}
	if len(dagTasks) != N {
		t.Errorf("Expected %d dag run tasks, got: %d", N, len(dagTasks))
	}

	for _, dagTask := range dagTasks {
		if dagTask.ExecTs != execTs {
			t.Errorf("Expected ExecTs=%s, got: %s", execTs, dagTask.ExecTs)
		}
		if dagTask.Status != DagRunTaskStatusScheduled {
			t.Errorf("Expeted status=%s, got: %s", DagRunTaskStatusScheduled,
				dagTask.Status)
		}
	}
}

func TestReadDagRunTaskSingleFromEmpty(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	_, rErr := c.ReadDagRunTask(ctx, "any_dag", "any_time", "any_task")
	if rErr != sql.ErrNoRows {
		t.Errorf("Expected no rows error, got: %s", rErr.Error())
	}
}

func TestReadDagRunTaskSingle(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	dagId := "test_dag_1"
	execTs := timeutils.ToString(time.Now())
	taskId := "my_task_1"
	ctx := context.Background()
	insertDagRunTask(c, ctx, dagId, execTs, taskId, t)

	drt, rErr := c.ReadDagRunTask(ctx, dagId, execTs, taskId)
	if rErr != nil {
		t.Errorf("Unexpected error while reading dagruntask: %s", rErr.Error())
	}
	if dagId != drt.DagId {
		t.Errorf("Expected dagId=%s, got: %s", dagId, drt.DagId)
	}
	if execTs != drt.ExecTs {
		t.Errorf("Expected execTs=%s, got: %s", execTs, drt.ExecTs)
	}
	if taskId != drt.TaskId {
		t.Errorf("Expected taskId=%s, got: %s", taskId, drt.TaskId)
	}
}

func TestReadDagRunTaskUpdate(t *testing.T) {
	c, err := NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Error(err)
	}
	dagId := "test_dag_1"
	execTs := timeutils.ToString(time.Now())
	taskId := "my_task_1"
	ctx := context.Background()
	insertDagRunTask(c, ctx, dagId, execTs, taskId, t)

	drt, rErr := c.ReadDagRunTask(ctx, dagId, execTs, taskId)
	if rErr != nil {
		t.Errorf("Unexpected error while reading dagruntask: %s", rErr.Error())
	}
	if dagId != drt.DagId {
		t.Errorf("Expected dagId=%s, got: %s", dagId, drt.DagId)
	}
	if execTs != drt.ExecTs {
		t.Errorf("Expected execTs=%s, got: %s", execTs, drt.ExecTs)
	}
	if taskId != drt.TaskId {
		t.Errorf("Expected taskId=%s, got: %s", taskId, drt.TaskId)
	}
	if drt.Status != DagRunTaskStatusScheduled {
		t.Errorf("Expected status: %s, got: %s", DagRunTaskStatusScheduled,
			drt.Status)
	}

	const newStatus = "NEW_STATUS"
	uErr := c.UpdateDagRunTaskStatus(ctx, dagId, execTs, taskId, newStatus)
	if uErr != nil {
		t.Errorf("Error while updating dag run task status: %s", uErr.Error())
	}

	drt2, rErr2 := c.ReadDagRunTask(ctx, dagId, execTs, taskId)
	if rErr2 != nil {
		t.Errorf("Unexpected error while reading dagruntask: %s", rErr.Error())
	}
	if drt2.Status != newStatus {
		t.Errorf("Expected status after update: %s, got: %s", newStatus,
			drt2.Status)
	}
	sTime1 := timeutils.FromStringMust(drt.StatusUpdateTs)
	sTime2 := timeutils.FromStringMust(drt2.StatusUpdateTs)
	if sTime1.Compare(sTime2) > 0 {
		t.Errorf("Expected new update timestamp %v to be later than %v",
			sTime2, sTime1)
	}
}

func TestReadDagRunTasksNotFinishedEmpty(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Error(err)
	}
	defer CleanUpSqliteTmp(c, t)

	// do not insert data intentionally - dagruntasks is empty
	ctx := context.Background()
	drts, err := c.ReadDagRunTasksNotFinished(ctx)
	if err != nil {
		t.Errorf("Error while reading not finished DAG run tasks: %s",
			err.Error())
	}
	if len(drts) != 0 {
		t.Errorf("Expected no results based on empty table, got %d tasks",
			len(drts))
	}
}

func TestReadDagRunTasksNotFinishedSimple(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Error(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ctx := context.Background()
	dId := "mock_dag_1"
	ts := timeutils.ToString(time.Now())

	inputData := []struct {
		taskId string
		status dag.TaskStatus
	}{
		{"task_1", dag.TaskSuccess},
		{"task_2", dag.TaskSuccess},
		{"task_3", dag.TaskRunning},
		{"task_4", dag.TaskFailed},
	}

	// Insert data
	for _, d := range inputData {
		insertDagRunTask(c, ctx, dId, ts, d.taskId, t)
		uErr := c.UpdateDagRunTaskStatus(
			ctx, dId, ts, d.taskId, d.status.String(),
		)
		if uErr != nil {
			t.Errorf("Cannot update DAG run task status for %s: %s",
				d.taskId, uErr.Error())
		}
	}

	drts, err := c.ReadDagRunTasksNotFinished(ctx)
	if err != nil {
		t.Errorf("Error while reading not finished DAG run tasks: %s",
			err.Error())
	}
	if len(drts) != 1 {
		t.Fatalf("Expected 1 not finished task, got %d (%+v)",
			len(drts), drts)
	}
	if drts[0].TaskId != "task_3" {
		t.Errorf("Expected not finished DAG run task id to be %s, got %s",
			"task_3", drts[0].TaskId)
	}
}

func TestRunningTasksNumEmpty(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)

	ctx := context.Background()
	num, dErr := c.RunningTasksNum(ctx)
	if dErr != nil {
		t.Errorf("Error while RunningTasksNum: %s", dErr.Error())
	}
	if num != 0 {
		t.Errorf("Expected 0 running tasks on empty database, got: %d", num)
	}
}

func TestRunningTasksNumAllFinished(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)

	dags := []string{"mock_dag", "mock_dag_2"}
	ctx := context.Background()
	ts := timeutils.ToString(time.Now())

	data := []struct {
		dagId  string
		taskId string
		status string
	}{
		{dags[0], "task_1", statusSuccess},
		{dags[0], "task_2", statusSuccess},
		{dags[0], "task_3", statusSuccess},
		{dags[1], "task_1", statusSuccess},
		{dags[1], "task_2", statusFailed},
	}

	for _, d := range data {
		insertDagRunTaskStatus(c, ctx, d.dagId, ts, d.taskId, d.status, t)
	}

	num, dErr := c.RunningTasksNum(ctx)
	if dErr != nil {
		t.Errorf("Error while RunningTasksNum: %s", dErr.Error())
	}
	if num != 0 {
		t.Errorf("Expected 0 running tasks, but got %d", num)
	}
}

func TestRunningTasksNumWithRunningTasks(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)

	dags := []string{"mock_dag", "mock_dag_2"}
	ctx := context.Background()
	ts := timeutils.ToString(time.Now())

	data := []struct {
		dagId  string
		taskId string
		status string
	}{
		{dags[0], "task_1", statusSuccess},
		{dags[0], "task_2", statusRunning},
		{dags[0], "task_3", statusRunning},
		{dags[1], "task_1", statusSuccess},
		{dags[1], "task_2", statusRunning},
	}

	for _, d := range data {
		insertDagRunTaskStatus(c, ctx, d.dagId, ts, d.taskId, d.status, t)
	}

	num, dErr := c.RunningTasksNum(ctx)
	if dErr != nil {
		t.Errorf("Error while RunningTasksNum: %s", dErr.Error())
	}
	if num != 3 {
		t.Errorf("Expected 3 running tasks, got: %d", num)
	}
}

func TestRunningTasksNumWithUpdate(t *testing.T) {
	c, err := NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)

	dagId := "mock_dag"
	ctx := context.Background()
	ts := timeutils.ToString(time.Now())

	data := []struct {
		taskId string
		status string
	}{
		{"task_1", statusSuccess},
		{"task_2", statusSuccess},
		{"task_3", statusRunning},
	}

	for _, d := range data {
		insertDagRunTaskStatus(c, ctx, dagId, ts, d.taskId, d.status, t)
	}

	num1, dErr := c.RunningTasksNum(ctx)
	if dErr != nil {
		t.Errorf("Error while RunningTasksNum: %s", dErr.Error())
	}
	if num1 != 1 {
		t.Errorf("Expected 1 running task, got: %d", num1)
	}

	uErr := c.UpdateDagRunTaskStatus(ctx, dagId, ts, "task_3", statusSuccess)
	if uErr != nil {
		t.Errorf("Error when updating DAG run task status: %s", uErr.Error())
	}

	num2, dErr2 := c.RunningTasksNum(ctx)
	if dErr2 != nil {
		t.Errorf("Error while RunningTasksNum: %s", dErr.Error())
	}
	if num2 != 0 {
		t.Errorf("Expected 0 running tasks, got: %d", num2)
	}
}

func insertDagRunTask(
	c *Client,
	ctx context.Context,
	dagId, execTs, taskId string,
	t *testing.T,
) {
	iErr := c.InsertDagRunTask(
		ctx, dagId, execTs, taskId, DagRunTaskStatusScheduled,
	)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
}

func insertDagRunTaskStatus(
	c *Client, ctx context.Context, dagId, execTs, taskId, status string,
	t *testing.T,
) {
	iErr := c.InsertDagRunTask(ctx, dagId, execTs, taskId, status)
	if iErr != nil {
		t.Errorf("Error while inserting dag run task: %s", iErr.Error())
	}
}
