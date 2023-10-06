package main

import (
	"context"
	"go_shed/src/dag"
	"go_shed/src/db"
	"go_shed/src/timeutils"
	"testing"
	"time"
)

func TestNextScheduleForDagRunsSimple(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	const dagRuns = 10
	const dagId = "mock_dag"
	ctx := context.Background()

	startTs := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: startTs}
	attr := dag.Attr{}
	d := emptyDag(dagId, &sched, attr)

	for i := 0; i < dagRuns; i++ {
		_, err := c.InsertDagRun(ctx, dagId, timeutils.ToString(startTs.Add(time.Duration(i)*time.Hour)))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := startTs.Add(time.Duration(dagRuns)*time.Hour + 45*time.Minute)
	nextSchedulesMap := nextScheduleForDagRuns(ctx, []dag.Dag{d}, currentTime, c)

	if len(nextSchedulesMap) != 1 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d", len(nextSchedulesMap))
	}

	nextSchedule, exists := nextSchedulesMap[d.Id]
	if !exists {
		t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not", dagId)
	}
	expectedNextSchedule := startTs.Add(time.Duration(dagRuns+1) * time.Hour)
	if nextSchedule.Compare(expectedNextSchedule) != 0 {
		t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
			dagId, currentTime, expectedNextSchedule, nextSchedule)
	}
}

func TestNextScheduleForDagRunsSimpleWithCatchUp(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	const dagRuns = 1
	const dagId = "mock_dag"
	ctx := context.Background()

	startTs := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: startTs}
	attr := dag.Attr{CatchUp: true}
	d := emptyDag(dagId, &sched, attr)

	for i := 0; i < dagRuns; i++ {
		_, err := c.InsertDagRun(ctx, dagId, timeutils.ToString(startTs.Add(time.Duration(i)*time.Hour)))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := time.Date(2023, time.October, 10, 10, 0, 0, 0, time.UTC)
	nextSchedulesMap := nextScheduleForDagRuns(ctx, []dag.Dag{d}, currentTime, c)

	if len(nextSchedulesMap) != 1 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d", len(nextSchedulesMap))
	}

	nextSchedule, exists := nextSchedulesMap[d.Id]
	if !exists {
		t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not", dagId)
	}
	expectedNextSchedule := startTs.Add(1 * time.Hour)
	if nextSchedule.Compare(expectedNextSchedule) != 0 {
		t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
			dagId, currentTime, expectedNextSchedule, nextSchedule)
	}
}

// TODO: More tests for NextScheduleForDagRuns

func emptyDag(dagId string, sched dag.Schedule, attr dag.Attr) dag.Dag {
	return dag.New(dag.Id(dagId)).AddSchedule(sched).AddAttributes(attr).Done()
}
