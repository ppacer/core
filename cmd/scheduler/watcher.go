package main

import (
	"go_shed/src/dag"
	"go_shed/src/db"
	"go_shed/src/timeutils"
	"time"

	"github.com/rs/zerolog/log"
)

const LOG_PREFIX = "scheduler"
const StatusReadyToSchedule = "READY_TO_SCHEDULE"
const StatusScheduled = "SCHEDULED"
const WatchInterval = 100 * time.Millisecond

type ScheduledInstance struct {
	Dag    dag.Dag
	AtTime time.Time
}

// Watch watches DAG registry and check whenever DAGs should be scheduled. In
// that case they are sent onto the given channel. Watch checks DAG registry
// every WatchInterval period. This function runs indefinitely.
func Watch(dags []dag.Dag, scheduleChan chan<- ScheduledInstance, dbClient *db.Client) {
	latestDagRunsCache := readLatestDagRunTimes(dbClient)
	for {
		trySchedule(dags, scheduleChan, latestDagRunsCache, dbClient)
		time.Sleep(WatchInterval)
	}
}

func trySchedule(dags []dag.Dag, scheduleChan chan<- ScheduledInstance, cache map[dag.Id]time.Time, dbClient *db.Client) {
	for _, d := range dags {
		if shouldBe, schedule := shouldBeSheduled(d, time.Now()); shouldBe {
			// Check the cache first
			if latestTs, isInCache := cache[d.Id]; isInCache && schedule.Equal(latestTs) {
				continue
			}
			execTs := timeutils.ToString(schedule)
			alreadyScheduled, dreErr := dbClient.DagRunExists(string(d.Id), execTs)
			if dreErr != nil {
				// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
			}
			if alreadyScheduled {
				continue
			}
			runId, iErr := dbClient.InsertDagRun(string(d.Id), execTs)
			if iErr != nil {
				// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
			}
			uErr := dbClient.UpdateDagRunStatus(runId, StatusReadyToSchedule)
			if uErr != nil {
				// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
			}
			scheduleChan <- ScheduledInstance{Dag: d, AtTime: schedule}
			uErr = dbClient.UpdateDagRunStatus(runId, StatusScheduled)
			if uErr != nil {
				// TODO: Think in general how do we want handle DB errors in here? Retry? Skip? How? Something generic?
			}
		}
	}
}

func shouldBeSheduled(dag dag.Dag, currentTime time.Time) (bool, time.Time) {
	if dag.Schedule == nil {
		return false, time.Time{}
	}
	sched := *dag.Schedule
	if sched.StartTime().Compare(currentTime) == 1 {
		// Given current time is before schedule start time
		return false, time.Time{}
	}
	if sched.Next(currentTime).Compare(currentTime) == -1 {
		// Next schedule tick is before current time and should be scheduled
		return true, sched.Next(currentTime)
	}
	return false, time.Time{}
}

// Reads from database latest dagrun for each DAG and caches its execution timestamps. In case of problems with
// database connection cache can be empty.
func readLatestDagRunTimes(dbClient *db.Client) map[dag.Id]time.Time {
	latestDagRunTime := make(map[dag.Id]time.Time)
	latestDagRuns, err := dbClient.ReadLatestDagRuns()
	if err != nil {
		log.Error().Msgf("[%s] Failed to load latest DAG runs to create a cache. Cache is empty.", LOG_PREFIX)
		return latestDagRunTime
	}
	for _, dagrun := range latestDagRuns {
		latestTs, tErr := timeutils.FromString(dagrun.ExecTs)
		if tErr != nil {
			log.Error().Str("dagId", dagrun.DagId).Int64("runId", dagrun.RunId).Str("ExecTs", dagrun.ExecTs).
				Msgf("[%s] Cannot deserialize dagrun execution timestamp (ExecTs)", LOG_PREFIX)
			continue
		}
		latestDagRunTime[dag.Id(dagrun.DagId)] = latestTs
	}
	return latestDagRunTime
}
