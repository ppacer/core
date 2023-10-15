package sched

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
const WatchInterval = 1 * time.Second
const QueueIsFullInterval = 100 * time.Millisecond

type DagRun struct {
	DagId  dag.Id
	AtTime time.Time
}

// Watch watches DAG registry and check whenever DAGs should be scheduled. In that case they are sent onto the given
// channel. Watch checks DAG registry every WatchInterval period. This function runs indefinitely.
func WatchDagRuns(dags []dag.Dag, queue ds.Queue[DagRun], dbClient *db.Client) {
	ctx := context.TODO() // Think about it
	nextSchedules := nextScheduleForDagRuns(ctx, dag.List(), time.Now(), dbClient)
	for {
		trySchedule(dags, queue, nextSchedules, dbClient)
		time.Sleep(WatchInterval)
	}
}

func trySchedule(dags []dag.Dag, queue ds.Queue[DagRun], nextSchedules map[dag.Id]*time.Time, dbClient *db.Client) {
	now := time.Now()

	// Make sure we have next schedules for every dag in dags
	if len(dags) != len(nextSchedules) {
		log.Warn().Int("dag", len(dags)).Int("nextSchedules", len(nextSchedules)).
			Msgf("Seems like number of next shedules does not match number of dags. Will try refresh next schedules.")
		ctx := context.TODO() // Think about it
		nextSchedules = nextScheduleForDagRuns(ctx, dags, now, dbClient)
	}

	for _, d := range dags {
		ctx := context.TODO() // Think about it

		// Check if there is space on the queue to schedule a new dag run
		for queue.Capacity() <= 0 {
			log.Warn().Msgf("The dag run queue is currently full. Will try again in %v", QueueIsFullInterval)
			time.Sleep(QueueIsFullInterval)
		}

		tsdErr := tryScheduleDag(ctx, d, now, queue, nextSchedules, dbClient)
		if tsdErr != nil {
			// TODO(dskrzypiec): implement some kind of second queue for retries when there was an error when trying to
			// schedule new dag run.
			log.Error().Err(tsdErr).Str("dagId", string(d.Id)).Msg("Error while trying to schedule new dag run")
		}
	}
}

// Function tryScheduleDag tries to schedule new dag run for given DAG. When new dag run is scheduled new entry is
// pushed onto the queue and into database dagruns table. When scheduling new dag run fails in any of steps, then
// non-nil error is returned and function try to clean up as much as possible. When necessary pop new entry from the
// queue and revert next schedule time from the cache (nextSchedules).
func tryScheduleDag(
	ctx context.Context,
	dag dag.Dag,
	currentTime time.Time,
	queue ds.Queue[DagRun],
	nextSchedules map[dag.Id]*time.Time,
	dbClient *db.Client,
) error {
	shouldBe, schedule := shouldBeSheduled(dag, nextSchedules, currentTime)
	if !shouldBe {
		return nil
	}
	log.Info().Str("dagId", string(dag.Id)).Time("execTs", schedule).Msg("About to try scheduling new dag run")

	execTs := timeutils.ToString(schedule)
	alreadyScheduled, dreErr := dbClient.DagRunExists(ctx, string(dag.Id), execTs)
	if dreErr != nil {
		return dreErr
	}
	if alreadyScheduled {
		alreadyInQueue := queue.Contains(DagRun{DagId: dag.Id, AtTime: schedule})
		if !alreadyInQueue {
			// When dag run is already in the database but it's not on the queue (perhaps due to scheduler restart).
			qErr := queue.Put(DagRun{DagId: dag.Id, AtTime: schedule})
			if qErr != nil {
				log.Error().Err(qErr).Str("dagId", string(dag.Id)).Time("execTime", schedule).
					Msg("Cannot put dag run on the queue")
				return qErr
			}
		}
		return nil
	}
	runId, iErr := dbClient.InsertDagRun(ctx, string(dag.Id), execTs)
	if iErr != nil {
		// Revert next schedule
		nextSchedules[dag.Id] = &schedule
		return iErr
	}
	uErr := dbClient.UpdateDagRunStatus(ctx, runId, db.DagRunStatusReadyToSchedule)
	if uErr != nil {
		log.Warn().Err(uErr).Str("dagId", string(dag.Id)).Time("execTime", schedule).
			Msg("Cannot update dag run status to READY_TO_SCHEDULE")
		// We don't need to block the process because of this error. In worst case we can omit having this status.
	}
	qErr := queue.Put(DagRun{DagId: dag.Id, AtTime: schedule})
	if qErr != nil {
		// Revert next schedule
		nextSchedules[dag.Id] = &schedule
		// We don't remove entry from dagruns table, based on this it would be put on the queue in the next iteration.
		log.Error().Err(qErr).Str("dagId", string(dag.Id)).Time("execTime", schedule).
			Msg("Cannot put dag run on the queue")
		return qErr
	}
	uErr = dbClient.UpdateDagRunStatus(ctx, runId, db.DagRunStatusScheduled)
	if uErr != nil {
		// Revert next schedule
		nextSchedules[dag.Id] = &schedule
		// Pop item from the queue
		_, pErr := queue.Pop()
		if pErr != nil && pErr != ds.ErrQueueIsEmpty {
			log.Error().Err(pErr).Msg("Cannot pop latest item from the queue")
		}
		return uErr
	}
	return nil
}

// Check if givne DAG should be scheduled at given current time. If it should be scheduled then true and execution time
// is returned.
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

// NextScheduleForDagRuns determines next schedules for all DAGs given in dags and current time. It reads latest dag
// runs from the database. If DAG has no schedule this DAG is still in output map but with nil next schedule time.
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
