// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/timeutils"
)

func TestSyncOneDagNoChanges(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	ctx := context.Background()
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(ctx, c, d, simpleLogger())
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s",
			rErr.Error())
	}
	dagtasksDb1, dtErr := c.ReadDagTasks(ctx, dagId)
	if dtErr != nil {
		t.Fatalf("Unexpected error while reading dag tasks from dagtasks table: %s",
			dtErr.Error())
	}
	if len(dagtasksDb1) != tasksNum {
		t.Fatalf("Expected %d dag tasks in dagtasks table, got: %d",
			tasksNum, len(dagtasksDb1))
	}

	// Second sync - should not change anything
	s2Err := syncDag(ctx, c, d, simpleLogger())
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb2, r2Err := c.ReadDag(ctx, dagId)
	if r2Err != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s",
			r2Err.Error())
	}

	dagtasksDb2, dt2Err := c.ReadDagTasks(ctx, dagId)
	if dt2Err != nil {
		t.Fatalf("Unexpected error while reading dag tasks from dagtasks table: %s",
			dt2Err.Error())
	}
	if len(dagtasksDb2) != tasksNum {
		t.Fatalf("Expected %d dag tasks in dagtasks table, got: %d",
			tasksNum, len(dagtasksDb1))
	}

	if !dagDb1.Equals(dagDb2) {
		t.Fatalf("Expected the same dag row from dags table after the second sync. After 1st: [%v], after 2nd: [%v]",
			dagDb1, dagDb2)
	}

	if len(dagtasksDb1) != len(dagtasksDb2) {
		t.Fatalf("Number of dagtasks changed after the second sync from %d, to %d",
			len(dagtasksDb1), len(dagtasksDb2))
	}

	for i := 0; i < len(dagtasksDb1); i++ {
		if dagtasksDb1[i] != dagtasksDb2[i] {
			t.Fatalf("Expected unchanged dagtasks after the second sync, got diff for i=%d: from [%v] to [%v]",
				i, dagtasksDb1[i], dagtasksDb2[i])
		}
	}
}

func TestSyncOneDagTimeout(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	sErr := syncDag(ctx, c, d, simpleLogger())
	if sErr == nil {
		t.Error("Expected syncDag error due to context timeout, but got nil")
	}
}

func TestSyncOneDagChangingAttr(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(ctx, c, d, simpleLogger())
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s",
			rErr.Error())
	}

	// Update DAG attributes
	d.Attr.Tags = []string{"test", "test2"}

	// Second sync - should not change anything
	s2Err := syncDag(ctx, c, d, simpleLogger())
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb2, r2Err := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag after the update from dags table: %s",
			r2Err.Error())
	}
	if dagDb1.Equals(dagDb2) {
		t.Fatal("Expected the row in dags table to be different after DAG's attr were updated")
	}
	if dagDb1.HashDagMeta == dagDb2.HashDagMeta {
		t.Errorf("Expected different HashDagMeta after the updated, but is unchanged: %s",
			dagDb1.HashDagMeta)
	}
	if dagDb1.Attributes == dagDb2.Attributes {
		t.Errorf("Expected different Attributes after the updated, but is unchanged: %s",
			dagDb1.Attributes)
	}
	if dagDb1.HashTasks != dagDb2.HashTasks {
		t.Errorf("Expected HashTasks to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.HashTasks, dagDb1.HashTasks)
	}
}

func TestSyncOneDagChangingSchedule(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	s1Err := syncDag(ctx, c, d, simpleLogger())
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	dagDb1, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s",
			rErr.Error())
	}

	// Update DAG schedule
	currentSched := *d.Schedule
	var newSched schedule.Schedule = schedule.NewFixed(
		currentSched.StartTime(),
		4*time.Hour,
	)
	d.Schedule = &newSched

	// Second sync - should not change anything
	s2Err := syncDag(ctx, c, d, simpleLogger())
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	dagDb2, r2Err := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag after the update from dags table: %s",
			r2Err.Error())
	}
	if dagDb1.Equals(dagDb2) {
		t.Fatal("Expected the row in dags table to be different after DAG's schedule update")
	}
	if dagDb1.HashDagMeta == dagDb2.HashDagMeta {
		t.Errorf("Expected different HashDagMeta after the updated, but is unchanged: %s",
			dagDb1.HashDagMeta)
	}
	if *dagDb1.Schedule == *dagDb2.Schedule {
		t.Errorf("Expected different schedule after the update, but got the same as earlier: %s",
			*dagDb1.Schedule)
	}
	if dagDb1.HashTasks != dagDb2.HashTasks {
		t.Errorf("Expected HashTasks to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.HashTasks, dagDb1.HashTasks)
	}
}

func TestSyncOneDagChangingTasks(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Before sync - db is empty
	dagtasksCountCheck(0, 0, c, t)

	// First sync
	dagId := "very_simple_dag"
	d := verySimpleDag(dagId)
	tasksNum := len(d.Flatten())
	s1Err := syncDag(ctx, c, d, simpleLogger())
	if s1Err != nil {
		t.Fatalf("Unexpected error while syncDag: %s", s1Err.Error())
		return
	}

	// Checks after the first sync
	dagtasksCountCheck(1, tasksNum, c, t)

	dagDb1, rErr := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag from dags table: %s",
			rErr.Error())
	}

	// Update DAG tasks
	additionalTask := dag.NewNode(printTask{Name: "bonus_task"})
	d.Root.Next(additionalTask)

	// Second sync - should not change anything
	s2Err := syncDag(ctx, c, d, simpleLogger())
	if s2Err != nil {
		t.Fatalf("Unexpected error while the second syncDag: %s", s2Err.Error())
	}

	// Checks after the second sync
	dagtasksCountCheck(1, tasksNum+tasksNum+1, c, t)

	currentDagTasks := c.CountWhere("dagtasks", "IsCurrent=1")
	if currentDagTasks != tasksNum+1 {
		t.Fatalf("Expected %d current rows in dagtasks table after the update, got: %d",
			tasksNum+1, currentDagTasks)
	}

	dagDb2, r2Err := c.ReadDag(ctx, dagId)
	if rErr != nil {
		t.Fatalf("Unexpected error while reading dag after the update from dags table: %s",
			r2Err.Error())
	}
	if dagDb1.Equals(dagDb2) {
		t.Fatal("Expected the row in dags table to be different after DAG's attr were updated")
	}
	if dagDb1.HashTasks == dagDb2.HashTasks {
		t.Errorf("Expected different HashTasks after the update, but is unchanged: %s",
			dagDb1.HashTasks)
	}
	if dagDb1.HashDagMeta != dagDb2.HashDagMeta {
		t.Errorf("Expected HashDagMeta to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.HashDagMeta, dagDb2.HashDagMeta)
	}
	if dagDb1.Attributes != dagDb2.Attributes {
		t.Errorf("Expected Attributes to be unchanged after the update, got different - before: %s, after: %s",
			dagDb1.Attributes, dagDb2.Attributes)
	}
}

func TestSyncDagRunTaskCacheEmpty(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	ctx := context.Background()
	ts := timeutils.ToString(time.Now())
	data := []struct {
		dagId  string
		taskId string
		status dag.TaskStatus
	}{
		{"dag1", "task1", dag.TaskSuccess},
		{"dag1", "end", dag.TaskSuccess},
		{"dag2", "start", dag.TaskFailed},
		{"dag3", "T", dag.TaskSuccess},
	}

	for _, d := range data {
		insertDagRunTask(c, ctx, d.dagId, ts, d.taskId, d.status.String(), t)
	}

	const size = 10
	drtCache := ds.NewLruCache[DagRunTask, DagRunTaskState](size)
	syncErr := syncDagRunTaskCache(drtCache, c, simpleLogger(), DefaultConfig)
	if syncErr != nil {
		t.Errorf("Error while syncing DAG run tasks cache: %s", syncErr.Error())
	}
	if drtCache.Len() != 0 {
		t.Errorf("Expected no items in the cache, got %d", drtCache.Len())
	}
}

func TestSyncDagRunTaskCacheSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	ctx := context.Background()
	ts := timeutils.ToString(time.Now())
	data := []struct {
		dagId  string
		taskId string
		status dag.TaskStatus
	}{
		{"dag1", "task1", dag.TaskSuccess},
		{"dag1", "task2", dag.TaskSuccess},
		{"dag1", "task3", dag.TaskRunning},
		{"dag2", "start", dag.TaskFailed},
		{"dag2", "end", dag.TaskUpstreamFailed},
		{"dag3", "T", dag.TaskRunning},
	}

	for _, d := range data {
		insertDagRunTask(c, ctx, d.dagId, ts, d.taskId, d.status.String(), t)
	}

	const size = 10
	const expected = 3
	drtCache := ds.NewLruCache[DagRunTask, DagRunTaskState](size)
	syncErr := syncDagRunTaskCache(drtCache, c, simpleLogger(), DefaultConfig)
	if syncErr != nil {
		t.Errorf("Error while syncing DAG run tasks cache: %s", syncErr.Error())
	}
	if drtCache.Len() != expected {
		t.Errorf("Expected %d items in the cache, got %d", expected,
			drtCache.Len())
	}

	expectedInCache := []struct {
		dagId  string
		taskId string
		status dag.TaskStatus
	}{
		{"dag1", "task3", dag.TaskRunning},
		{"dag2", "end", dag.TaskUpstreamFailed},
		{"dag3", "T", dag.TaskRunning},
	}
	for _, e := range expectedInCache {
		drts, exists := drtCache.Get(drt(e.dagId, ts, e.taskId))
		if !exists {
			t.Errorf("Expected DAG run task %s.%s.%s to be in the cache",
				e.dagId, ts, e.taskId)
		}
		if drts.Status != e.status {
			t.Errorf("Expected DAG run task %s.%s.%s to be in state: %s, but is: %s",
				e.dagId, ts, e.taskId, e.status.String(), drts.Status.String())
		}
	}
}

func TestSyncDagRunTaskCacheSmall(t *testing.T) {
	c, err := db.NewSqliteTmpClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const size = 2
	ctx := context.Background()
	ts := timeutils.ToString(time.Now())
	data := []struct {
		dagId  string
		taskId string
		status dag.TaskStatus
	}{
		{"dag1", "task1", dag.TaskSuccess},
		{"dag1", "task2", dag.TaskSuccess},
		{"dag1", "task3", dag.TaskRunning},
		{"dag2", "start", dag.TaskFailed},
		{"dag2", "end", dag.TaskUpstreamFailed},
		{"dag3", "T", dag.TaskRunning},
	}

	for _, d := range data {
		insertDagRunTask(c, ctx, d.dagId, ts, d.taskId, d.status.String(), t)
	}

	drtCache := ds.NewLruCache[DagRunTask, DagRunTaskState](size)
	syncErr := syncDagRunTaskCache(drtCache, c, simpleLogger(), DefaultConfig)
	if syncErr != nil {
		t.Errorf("Error while syncing DAG run tasks cache: %s", syncErr.Error())
	}
	if drtCache.Len() != size {
		t.Errorf("Expected %d items in the cache, got %d", size,
			drtCache.Len())
	}

	// dag1.ts.task3 should not be in the cache in case when cache size is 2.
	_, d1t3Exists := drtCache.Get(drt("dag1", ts, "task3"))
	if d1t3Exists {
		t.Errorf("DAG run task dag1.%s.task3 should not be in the cache, but it is",
			ts)
	}

	expectedInCache := []struct {
		dagId  string
		taskId string
		status dag.TaskStatus
	}{
		{"dag2", "end", dag.TaskUpstreamFailed},
		{"dag3", "T", dag.TaskRunning},
	}
	for _, e := range expectedInCache {
		drts, exists := drtCache.Get(drt(e.dagId, ts, e.taskId))
		if !exists {
			t.Errorf("Expected DAG run task %s.%s.%s to be in the cache",
				e.dagId, ts, e.taskId)
		}
		if drts.Status != e.status {
			t.Errorf("Expected DAG run task %s.%s.%s to be in state: %s, but is: %s",
				e.dagId, ts, e.taskId, e.status.String(), drts.Status.String())
		}
	}
}

func drt(dagId, execTs, taskId string) DagRunTask {
	return DagRunTask{
		DagId:  dag.Id(dagId),
		AtTime: timeutils.FromStringMust(execTs),
		TaskId: taskId,
	}
}

func dagtasksCountCheck(
	expDagCnt,
	expDagTasksCnt int,
	c *db.Client,
	t *testing.T,
) {
	dagCount := c.Count("dags")
	if dagCount != expDagCnt {
		t.Fatalf("Expected %d row in dags table, got: %d", expDagCnt, dagCount)
	}
	dagtasksCount := c.Count("dagtasks")
	if dagtasksCount != expDagTasksCnt {
		t.Fatalf("Expected %d rows in dagtasks table, got: %d", expDagTasksCnt,
			dagtasksCount)
	}
}

func insertDagRunTask(
	c *db.Client,
	ctx context.Context,
	dagId, execTs, taskId, status string,
	t *testing.T,
) {
	iErr := c.InsertDagRunTask(ctx, dagId, execTs, taskId, status)
	if iErr != nil {
		t.Errorf("Error while inserting dag run: %s", iErr.Error())
	}
}

func verySimpleDag(dagId string) dag.Dag {
	start := dag.NewNode(waitTask{
		TaskId:   "start",
		Interval: 3 * time.Second,
	})
	t := dag.NewNode(printTask{Name: "t_1"})
	end := dag.NewNode(waitTask{
		TaskId:   "end",
		Interval: 1 * time.Second,
	})
	start.Next(t).Next(end)

	startTs := time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC)
	sched := schedule.NewFixed(startTs, 5*time.Minute)
	attr := dag.Attr{Tags: []string{"test"}}
	dag := dag.New(dag.Id(dagId)).
		AddSchedule(&sched).
		AddRoot(start).
		AddAttributes(attr).
		Done()
	return dag
}

type printTask struct {
	Name string
}

func (pt printTask) Id() string { return pt.Name }

func (pt printTask) Execute(_ dag.TaskContext) error {
	fmt.Println("Hello executor!")
	return nil
}

// WaitTask is a Task which just waits and logs.
type waitTask struct {
	TaskId   string
	Interval time.Duration
}

func (wt waitTask) Id() string { return wt.TaskId }

func (wt waitTask) Execute(_ dag.TaskContext) error {
	l := simpleLogger()
	l.Info("Start sleeping", "task", wt.Id(), "interval", wt.Interval)
	time.Sleep(wt.Interval)
	l.Info("Task is done", "task", wt.Id())
	return nil
}

func simpleLogger() *slog.Logger {
	opts := slog.HandlerOptions{Level: slog.LevelInfo}
	return slog.New(slog.NewTextHandler(os.Stdout, &opts))
}
