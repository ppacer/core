package main

import (
	"context"
	"fmt"
	"go_shed/src/dag"
	"go_shed/src/db"
	"go_shed/src/timeutils"
	"math/rand"
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

func TestNextScheduleForDagRunsManyDagsSimple(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	attr := dag.Attr{}

	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}
	sched2 := dag.FixedSchedule{Interval: 5 * time.Hour, Start: start}
	sched3 := dag.FixedSchedule{Interval: 10 * time.Minute, Start: start}

	d1 := emptyDag("dag1", &sched1, attr)
	d2 := emptyDag("dag2", &sched2, attr)
	d3 := emptyDag("dag3", &sched3, attr)

	for _, dagId := range []string{"dag1", "dag2", "dag3"} {
		_, err := c.InsertDagRun(ctx, dagId, timeutils.ToString(start))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := start.Add(5 * time.Minute)
	nextSchedulesMap := nextScheduleForDagRuns(ctx, []dag.Dag{d1, d2, d3}, currentTime, c)

	if len(nextSchedulesMap) != 3 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d", len(nextSchedulesMap))
	}

	expectedNextScheds := []time.Time{
		start.Add(1 * time.Hour),
		start.Add(5 * time.Hour),
		start.Add(10 * time.Minute),
	}

	for idx, d := range []dag.Dag{d1, d2, d3} {
		nextSched, exists := nextSchedulesMap[d.Id]
		if !exists {
			t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not", string(d.Id))
		}
		if nextSched.Compare(expectedNextScheds[idx]) != 0 {
			t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
				string(d.Id), currentTime, expectedNextScheds[idx], nextSched)
		}
	}
}

func TestNextScheduleForDagRunsBeforeStart(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	dagNumber := 100
	ctx := context.Background()
	dags := make([]dag.Dag, 0, dagNumber)
	attr := dag.Attr{}

	for i := 0; i < dagNumber; i++ {
		start := timeutils.RandomUtcTime(2010)
		h := rand.Intn(1000)
		sched := dag.FixedSchedule{Interval: time.Duration(h) * time.Hour, Start: start}
		d := emptyDag(fmt.Sprintf("d_%d", i), &sched, attr)
		dags = append(dags, d)
	}

	currentTime := time.Date(2008, time.October, 5, 12, 0, 0, 0, time.UTC)
	nextSchedulesMap := nextScheduleForDagRuns(ctx, dags, currentTime, c)

	if len(nextSchedulesMap) != dagNumber {
		t.Errorf("Expected to got next schedule for %d DAGs, got for %d", dagNumber, len(nextSchedulesMap))
	}

	for _, d := range dags {
		nextSched, exists := nextSchedulesMap[d.Id]
		if !exists {
			t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not", string(d.Id))
		}
		expectedNextSched := (*d.Schedule).StartTime()
		if nextSched.Compare(expectedNextSched) != 0 {
			t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
				string(d.Id), currentTime, expectedNextSched, nextSched)
		}
	}
}

func TestNextScheduleForDagRunsNoSchedule(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	d1 := dag.New(dag.Id("d1")).Done()
	d2 := dag.New(dag.Id("s2")).Done()

	currentTime := time.Date(2008, time.October, 5, 12, 0, 0, 0, time.UTC)
	nextSchedulesMap := nextScheduleForDagRuns(ctx, []dag.Dag{d1, d2}, currentTime, c)

	if len(nextSchedulesMap) != 2 {
		t.Errorf("Expected to got next schedule for %d DAGs, got for %d", 2, len(nextSchedulesMap))
	}

	for dagId, nextSched := range nextSchedulesMap {
		if nextSched != nil {
			t.Errorf("Expected nil next schedule for %s DAG, got %v", string(dagId), nextSched)
		}
	}
}

func emptyDag(dagId string, sched dag.Schedule, attr dag.Attr) dag.Dag {
	return dag.New(dag.Id(dagId)).AddSchedule(sched).AddAttributes(attr).Done()
}
