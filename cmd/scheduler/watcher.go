package main

import (
	"context"
	"go_shed/src/dag"
	"go_shed/src/db"
	"go_shed/src/ds"
	"go_shed/src/timeutils"
	"time"

	"github.com/rs/zerolog/log"
)

const LOG_PREFIX = "scheduler"
const StatusReadyToSchedule = "READY_TO_SCHEDULE"
const StatusScheduled = "SCHEDULED"
const WatchInterval = 1 * time.Second

type DagRun struct {
	DagId  dag.Id
	AtTime time.Time
}

// Watch watches DAG registry and check whenever DAGs should be scheduled. In
// that case they are sent onto the given channel. Watch checks DAG registry
// every WatchInterval period. This function runs indefinitely.
func Watch(dags []dag.Dag, queue ds.Queue[DagRun], dbClient *db.Client) {
	ctx := context.TODO() // Think about it
	nextSchedules := nextScheduleForDagRuns(ctx, dag.List(), time.Now(), dbClient)
	for {
		trySchedule(dags, queue, nextSchedules, dbClient)
		time.Sleep(WatchInterval)
	}
}

func trySchedule(dags []dag.Dag, queue ds.Queue[DagRun], cache map[dag.Id]*time.Time, dbClient *db.Client) {
	for _, d := range dags {
		ctx := context.TODO() // Think about it
		tsdErr := tryScheduleDag(ctx, d, queue, cache, dbClient)
		if tsdErr != nil {
			log.Error().Err(tsdErr).Str("dagId", string(d.Id)).Msg("Error while trying to schedule dag")
		}
	}
}

func tryScheduleDag(
	ctx context.Context, dag dag.Dag, queue ds.Queue[DagRun], nextSchedules map[dag.Id]*time.Time, dbClient *db.Client,
) error {
	shouldBe, schedule := shouldBeSheduled(dag, nextSchedules, time.Now())
	if !shouldBe {
		return nil
	}
	log.Info().Str("dagId", string(dag.Id)).Time("execTs", schedule).Msg("About to try scheduling new dag run")

	// Check the cache first
	if latestTs, isInCache := nextSchedules[dag.Id]; isInCache && schedule.Equal(*latestTs) {
		return nil
	}
	execTs := timeutils.ToString(schedule)
	alreadyScheduled, dreErr := dbClient.DagRunExists(ctx, string(dag.Id), execTs)
	if dreErr != nil {
		// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
		return dreErr
	}
	if alreadyScheduled {
		return nil
	}
	runId, iErr := dbClient.InsertDagRun(ctx, string(dag.Id), execTs)
	if iErr != nil {
		// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
		return iErr
	}
	uErr := dbClient.UpdateDagRunStatus(ctx, runId, StatusReadyToSchedule)
	if uErr != nil {
		// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
		return uErr
	}
	if queue.Capacity() <= 0 {
		// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
		return ds.ErrQueueIsFull
	}
	qErr := queue.Put(DagRun{DagId: dag.Id, AtTime: schedule})
	if qErr != nil {
		return qErr
	}
	uErr = dbClient.UpdateDagRunStatus(ctx, runId, StatusScheduled)
	if uErr != nil {
		// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
		return uErr
	}
	return nil
}

func shouldBeSheduled(dag dag.Dag, nextSchedules map[dag.Id]*time.Time, currentTime time.Time) (bool, time.Time) {
	if dag.Schedule == nil {
		return false, time.Time{}
	}
	nextSched, exists := nextSchedules[dag.Id]
	if !exists {
		return false, time.Time{}
	}
	if nextSched == nil {
		return false, time.Time{}
	}
	if (*nextSched).Compare(currentTime) <= 0 {
		// Update next schedule and return the current schedule
		newNextSched := (*dag.Schedule).Next(*nextSched)
		nextSchedules[dag.Id] = &newNextSched
		return true, *nextSched
	}
	return false, time.Time{}
}

// TODO: docs + tests
func nextScheduleForDagRuns(ctx context.Context, dags []dag.Dag, currentTime time.Time, dbClient *db.Client) map[dag.Id]*time.Time {
	latestDagRunTime := make(map[dag.Id]*time.Time, len(dags))
	latestDagRuns, err := dbClient.ReadLatestDagRuns(ctx)
	if err != nil {
		log.Error().Msgf("[%s] Failed to load latest DAG runs to create a cache. Cache is empty.", LOG_PREFIX)
		return latestDagRunTime
	}

	for _, dag := range dags {
		if dag.Schedule == nil {
			// No schedule for this DAG
			latestDagRunTime[dag.Id] = nil
			continue
		}
		sched := *dag.Schedule
		startTime := sched.StartTime()
		latestDagRun, exists := latestDagRuns[string(dag.Id)]
		if !exists {
			// The first run
			if dag.Attr.CatchUp {
				latestDagRunTime[dag.Id] = &startTime
				continue
			} else {
				nextSched := sched.Next(currentTime)
				latestDagRunTime[dag.Id] = &nextSched
				continue
			}
		}
		nextSched := sched.Next(timeutils.FromStringMust(latestDagRun.ExecTs))
		if currentTime.Compare(nextSched) == 1 && !dag.Attr.CatchUp {
			// current time is after the supposed schedule and we don't want to catch up
			nextSchedFromCurrent := sched.Next(currentTime)
			latestDagRunTime[dag.Id] = &nextSchedFromCurrent
			continue
		}
		latestDagRunTime[dag.Id] = &nextSched
	}

	return latestDagRunTime
}
