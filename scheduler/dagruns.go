// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/timeutils"
)

// DagRun represents single DAG run. AtTime is more of scheduling time rather
// then time of actually pushing it onto DAG run queue.
type DagRun struct {
	DagId  dag.Id
	AtTime time.Time
}

// DagRunWatcher watches on given list of DAGs and sends information about new
// DAG runs onto the internal queue when it's time for a new DAG run to be
// scheduled. Next DAG run schedule for given DAG is determined based on its
// schedule. It also synchronize information about DAG run with the database.
//
// If you use Scheduler, you probably don't need to use this object directly.
type DagRunWatcher struct {
	queue              ds.Queue[DagRun]
	dbClient           *db.Client
	schedulerStateFunc GetStateFunc
	config             DagRunWatcherConfig
}

// NewDagRunWatcher creates new instance of DagRunWatcher.
func NewDagRunWatcher(queue ds.Queue[DagRun], dbClient *db.Client, stateFunc GetStateFunc, config DagRunWatcherConfig) *DagRunWatcher {
	return &DagRunWatcher{
		queue:              queue,
		dbClient:           dbClient,
		schedulerStateFunc: stateFunc,
		config:             config,
	}
}

// Watch watches on given list of DAGs and try to schedules new DAG runs onto
// internal queue. Watch tries to do it each WatchInterval. In case when DAG
// run queue is full and new DAG runs cannot be pushed there Watch waits for
// DagRunWatcherConfig.QueueIsFullInterval before the next try. Even when the
// queue would be full for longer then an interval between two next DAG runs,
// those DAG runs won't be skipped. They will be scheduled in expected order
// but possibly a bit later.
func (drw *DagRunWatcher) Watch(dags dag.Registry) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, drw.config.DatabaseContextTimeout)
	defer cancel()
	nextSchedules := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, dags, time.Now(), drw.dbClient, nextSchedules)
	for {
		if drw.schedulerStateFunc() == StateStopping {
			slog.Warn("Scheduler is stopping. DagRunWatcher will not schedule other runs.")
			return
		}
		now := time.Now()
		trySchedule(dags, drw.queue, nextSchedules, now, drw.dbClient, drw.config)
		time.Sleep(drw.config.WatchInterval)
	}
}

func trySchedule(
	dags dag.Registry,
	queue ds.Queue[DagRun],
	nextSchedules map[dag.Id]*time.Time,
	currentTime time.Time,
	dbClient *db.Client,
	config DagRunWatcherConfig,
) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, config.DatabaseContextTimeout)
	defer cancel()

	// Make sure we have next schedules for every dag in dags
	if len(dags) != len(nextSchedules) {
		slog.Warn("Seems like number of next schedules does not match number "+
			"of dags. Will try refresh next schedules.", "dags", len(dags),
			"nextSchedules", len(nextSchedules))
		updateNextSchedules(ctx, dags, currentTime, dbClient, nextSchedules)
	}

	for _, d := range dags {
		// Check if there is space on the queue to schedule a new dag run
		for queue.Capacity() <= 0 {
			slog.Warn("The dag run queue is full. Will try in moment", "moment",
				config.QueueIsFullInterval)
			time.Sleep(config.QueueIsFullInterval)
		}

		tsdErr := tryScheduleDag(ctx, d, currentTime, queue, nextSchedules, dbClient)
		if tsdErr != nil {
			slog.Error("Error while trying to schedule new dag run", "dagId",
				string(d.Id), "err", tsdErr)
			// We don't need to handle those errors in here. Function
			// tryScheduleDag will be retried in another iteration and that
			// should be good enough to resolve all cases eventually.
		}
	}
}

// Function tryScheduleDag tries to schedule new dag run for given DAG. When
// new dag run is scheduled new entry is pushed onto the queue and into
// database dagruns table. When scheduling new dag run fails in any of steps,
// then non-nil error is returned. If DAG run is successfully scheduled, this
// function updates nextSchedules for given dag ID.
func tryScheduleDag(
	ctx context.Context,
	d dag.Dag,
	currentTime time.Time,
	queue ds.Queue[DagRun],
	nextSchedules map[dag.Id]*time.Time,
	dbClient *db.Client,
) error {
	shouldBe, schedule := shouldBeSheduled(d, nextSchedules, currentTime)
	if !shouldBe {
		return nil
	}
	slog.Info("About to try scheduling new dag run", "dagId", string(d.Id),
		"execTs", schedule)

	execTs := timeutils.ToString(schedule)
	newDagRun := DagRun{DagId: d.Id, AtTime: schedule}
	alreadyScheduled, dreErr := dbClient.DagRunAlreadyScheduled(ctx, string(d.Id), execTs)
	if dreErr != nil {
		return dreErr
	}
	if alreadyScheduled {
		alreadyInQueue := queue.Contains(newDagRun)
		if !alreadyInQueue {
			// When dag run is already in the database but it's not on the
			// queue (perhaps due to scheduler restart).
			ds.PutContext(ctx, queue, newDagRun)
		}
		return nil
	}
	runId, iErr := dbClient.InsertDagRun(ctx, string(d.Id), execTs)
	if iErr != nil {
		return iErr
	}
	uErr := dbClient.UpdateDagRunStatus(
		ctx, runId, dag.RunReadyToSchedule.String(),
	)
	if uErr != nil {
		slog.Warn("Cannot update dag run status to READY_TO_SCHEDULE", "dagId",
			string(d.Id), "execTs", schedule, "err", uErr)
		// We don't need to block the process because of this error. In worst
		// case we can omit having this status.
	}
	qErr := queue.Put(newDagRun)
	if qErr != nil {
		// We don't remove entry from dagruns table, based on this it would be
		// put on the queue in the next iteration.
		slog.Error("Cannot put dag run on the queue", "dagId", string(d.Id),
			"execTs", schedule, "err", qErr)
		return qErr
	}
	uErr = dbClient.UpdateDagRunStatus(ctx, runId, dag.RunScheduled.String())
	if uErr != nil {
		slog.Warn("Cannot update dag run status to SCHEDULED", "dagId",
			string(d.Id), "execTs", schedule, "err", uErr)
		return uErr
	}
	// Update the next schedule for that DAG
	newNextSched := (*d.Schedule).Next(schedule)
	nextSchedules[d.Id] = &newNextSched
	return nil
}

// Check if givne DAG should be scheduled at given current time. If it should
// be scheduled then true and execution time is returned.
func shouldBeSheduled(
	dag dag.Dag,
	nextSchedules map[dag.Id]*time.Time,
	currentTime time.Time,
) (bool, time.Time) {
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
		return true, *nextSched
	}
	return false, time.Time{}
}

// NextScheduleForDagRuns determines next schedules for all DAGs given in dags
// and current time. It reads latest dag runs from the database. If DAG has no
// schedule this DAG is still in output map but with nil next schedule time.
func updateNextSchedules(
	ctx context.Context,
	dags dag.Registry,
	currentTime time.Time,
	dbClient *db.Client,
	nextSchedules map[dag.Id]*time.Time,
) {
	latestDagRuns, err := dbClient.ReadLatestDagRuns(ctx)
	if err != nil {
		slog.Error("Failed to load latest dag runs to create a cache. Cache is empty")
	}

	for _, dag := range dags {
		if dag.Schedule == nil {
			// No schedule for this DAG
			nextSchedules[dag.Id] = nil
			continue
		}
		sched := *dag.Schedule
		startTime := sched.StartTime()
		latestDagRun, exists := latestDagRuns[string(dag.Id)]
		if !exists {
			// The first run
			if dag.Attr.CatchUp {
				nextSchedules[dag.Id] = &startTime
				continue
			} else {
				nextSched := sched.Next(currentTime)
				nextSchedules[dag.Id] = &nextSched
				continue
			}
		}
		nextSched := sched.Next(timeutils.FromStringMust(latestDagRun.ExecTs))
		nextSchedules[dag.Id] = &nextSched
	}
}
