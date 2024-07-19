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
	logger             *slog.Logger
	schedulerStateFunc GetStateFunc
	config             DagRunWatcherConfig
}

// NewDagRunWatcher creates new instance of DagRunWatcher. Argument stateFunc
// is usually passed by the main Scheduler, to give access to its state. In
// case when nil is provided as logger, then slog.Logger is instantiated with
// TextHandler and INFO severity level.
func NewDagRunWatcher(queue ds.Queue[DagRun], dbClient *db.Client, logger *slog.Logger, stateFunc GetStateFunc, config DagRunWatcherConfig) *DagRunWatcher {
	if logger == nil {
		logger = defaultLogger()
	}
	return &DagRunWatcher{
		queue:              queue,
		dbClient:           dbClient,
		logger:             logger,
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
func (drw *DagRunWatcher) Watch(ctx context.Context, dags dag.Registry) {
	queryCtx, queryCancel := context.WithTimeout(
		ctx, drw.config.DatabaseContextTimeout,
	)
	nextSchedules := make(map[dag.Id]*time.Time)
	drw.updateNextSchedules(queryCtx, dags, timeutils.Now(), nextSchedules)
	queryCancel() // to avoid context leak

	for {
		select {
		case <-ctx.Done():
			drw.logger.Info(
				"DagRunWatcher is about to stop running. Context is done.",
				"ctx.Err", ctx.Err().Error())
			return
		default:
		}
		if drw.schedulerStateFunc() == StateStopping {
			drw.logger.Info(
				"Scheduler is stopping. DagRunWatcher will not schedule other runs.",
			)
			return
		}
		now := timeutils.Now()
		drw.trySchedule(dags, nextSchedules, now)
		time.Sleep(drw.config.WatchInterval)
	}
}

func (drw *DagRunWatcher) trySchedule(
	dags dag.Registry,
	nextSchedules map[dag.Id]*time.Time,
	currentTime time.Time,
) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, drw.config.DatabaseContextTimeout)
	defer cancel()

	// Make sure we have next schedules for every dag in dags
	if len(dags) != len(nextSchedules) {
		drw.logger.Warn("Seems like number of next schedules does not match number "+
			"of dags. Will try refresh next schedules.", "dags", len(dags),
			"nextSchedules", len(nextSchedules))
		drw.updateNextSchedules(ctx, dags, currentTime, nextSchedules)
	}

	for _, d := range dags {
		// Check if there is space on the queue to schedule a new dag run
		for drw.queue.Capacity() <= 0 {
			drw.logger.Warn("The dag run queue is full. Will try in moment", "moment",
				drw.config.QueueIsFullInterval)
			time.Sleep(drw.config.QueueIsFullInterval)
		}

		tsdErr := drw.tryScheduleDag(ctx, d, currentTime, nextSchedules)
		if tsdErr != nil {
			drw.logger.Error("Error while trying to schedule new dag run", "dagId",
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
func (drw *DagRunWatcher) tryScheduleDag(
	ctx context.Context,
	d dag.Dag,
	currentTime time.Time,
	nextSchedules map[dag.Id]*time.Time,
) error {
	shouldBe, schedule := shouldBeSheduled(d, nextSchedules, currentTime)
	if !shouldBe {
		return nil
	}
	drw.logger.Info("About to try scheduling new dag run", "dagId", string(d.Id),
		"execTs", schedule)

	execTs := timeutils.ToString(schedule)
	newDagRun := DagRun{DagId: d.Id, AtTime: schedule}
	alreadyScheduled, dreErr := drw.dbClient.DagRunAlreadyScheduled(ctx, string(d.Id), execTs)
	if dreErr != nil {
		return dreErr
	}
	if alreadyScheduled {
		alreadyInQueue := drw.queue.Contains(newDagRun)
		if !alreadyInQueue {
			// When dag run is already in the database but it's not on the
			// queue (perhaps due to scheduler restart).
			ds.PutContext(ctx, drw.queue, newDagRun)
		}
		return nil
	}
	runId, iErr := drw.dbClient.InsertDagRun(ctx, string(d.Id), execTs)
	if iErr != nil {
		return iErr
	}
	uErr := drw.dbClient.UpdateDagRunStatus(
		ctx, runId, dag.RunReadyToSchedule.String(),
	)
	if uErr != nil {
		drw.logger.Warn("Cannot update dag run status to READY_TO_SCHEDULE",
			"dagId", string(d.Id), "execTs", schedule, "err", uErr)
		// We don't need to block the process because of this error. In worst
		// case we can omit having this status.
	}
	qErr := drw.queue.Put(newDagRun)
	if qErr != nil {
		// We don't remove entry from dagruns table, based on this it would be
		// put on the queue in the next iteration.
		drw.logger.Error("Cannot put dag run on the queue", "dagId", string(d.Id),
			"execTs", schedule, "err", qErr)
		return qErr
	}
	uErr = drw.dbClient.UpdateDagRunStatus(ctx, runId, dag.RunScheduled.String())
	if uErr != nil {
		drw.logger.Warn("Cannot update dag run status to SCHEDULED", "dagId",
			string(d.Id), "execTs", schedule, "err", uErr)
		return uErr
	}
	// Update the next schedule for that DAG
	newNextSched := (*d.Schedule).Next(schedule, &schedule)
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
func (drw *DagRunWatcher) updateNextSchedules(
	ctx context.Context,
	dags dag.Registry,
	currentTime time.Time,
	nextSchedules map[dag.Id]*time.Time,
) {
	latestDagRuns, err := drw.dbClient.ReadLatestDagRuns(ctx)
	if err != nil {
		drw.logger.Error("Failed to load latest dag runs to create a cache. Cache is empty")
	}

	for _, dag := range dags {
		if dag.Schedule == nil {
			// No schedule for this DAG
			nextSchedules[dag.Id] = nil
			continue
		}
		sched := *dag.Schedule
		startTime := sched.Start()
		latestDagRun, exists := latestDagRuns[string(dag.Id)]
		if !exists {
			// The first run
			if dag.Attr.CatchUp {
				nextSchedules[dag.Id] = &startTime
				continue
			} else {
				nextSched := sched.Next(currentTime, nil)
				nextSchedules[dag.Id] = &nextSched
				continue
			}
		}
		prevSchedule := timeutils.FromStringMust(latestDagRun.ExecTs)
		nextSched := sched.Next(currentTime, &prevSchedule)
		nextSchedules[dag.Id] = &nextSched
	}
}
