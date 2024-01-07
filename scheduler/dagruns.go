package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/dskrzypiec/scheduler/dag"
	"github.com/dskrzypiec/scheduler/db"
	"github.com/dskrzypiec/scheduler/ds"
	"github.com/dskrzypiec/scheduler/timeutils"
)

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
	queue    ds.Queue[DagRun]
	dbClient *db.Client
	config   DagRunWatcherConfig
}

// NewDagRunWatcher creates new instance of DagRunWatcher.
func NewDagRunWatcher(queue ds.Queue[DagRun], dbClient *db.Client, config DagRunWatcherConfig) *DagRunWatcher {
	return &DagRunWatcher{
		queue:    queue,
		dbClient: dbClient,
		config:   config,
	}
}

// Watch watches on given list of DAGs and try to schedules new DAG runs onto
// internal queue. Watch tries to do it each WatchIntervalMs. In case when DAG
// run queue is full and new DAG runs cannot be pushed there Watch waits for
// DagRunWatcherConfig.QueueIsFullIntervalMs milliseconds before the next try.
// Even when the queue would be full for longer then interval between two next
// DAG runs, those DAG runs won't be skipped. They will be scheduled in
// expected order but possibly a bit later.
func (drw *DagRunWatcher) Watch(dags []dag.Dag) {
	ctx := context.TODO() // Think about it
	nextSchedules := nextScheduleForDagRuns(
		ctx, dags, time.Now(), drw.dbClient,
	)
	for {
		trySchedule(dags, drw.queue, nextSchedules, drw.dbClient, drw.config)
		time.Sleep(time.Duration(drw.config.WatchIntervalMs) * time.Millisecond)
	}
}

func trySchedule(
	dags []dag.Dag,
	queue ds.Queue[DagRun],
	nextSchedules map[dag.Id]*time.Time,
	dbClient *db.Client,
	config DagRunWatcherConfig,
) {
	now := time.Now()

	// Make sure we have next schedules for every dag in dags
	if len(dags) != len(nextSchedules) {
		slog.Warn("Seems like number of next shedules does not match number "+
			"of dags. Will try refresh next schedules.", "dags", len(dags),
			"nextSchedules", len(nextSchedules))
		ctx := context.TODO() // Think about it
		nextSchedules = nextScheduleForDagRuns(ctx, dags, now, dbClient)
	}

	for _, d := range dags {
		ctx := context.TODO() // Think about it

		// Check if there is space on the queue to schedule a new dag run
		for queue.Capacity() <= 0 {
			slog.Warn("The dag run queue is full. Will try in moment", "moment",
				config.QueueIsFullIntervalMs)
			time.Sleep(time.Duration(config.QueueIsFullIntervalMs) * time.Millisecond)
		}

		tsdErr := tryScheduleDag(ctx, d, now, queue, nextSchedules, dbClient)
		if tsdErr != nil {
			// TODO(dskrzypiec): implement some kind of second queue for
			// retries when there was an error when trying to schedule new dag
			// run.
			slog.Error("Error while trying to schedule new dag run", "dagId",
				string(d.Id), "err", tsdErr)
		}
	}
}

// Function tryScheduleDag tries to schedule new dag run for given DAG. When
// new dag run is scheduled new entry is pushed onto the queue and into
// database dagruns table. When scheduling new dag run fails in any of steps,
// then non-nil error is returned and function try to clean up as much as
// possible. When necessary pop new entry from the queue and revert next
// schedule time from the cache (nextSchedules).
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
			qErr := queue.Put(newDagRun)
			if qErr != nil {
				slog.Error("Cannot put dag run on the queue", "dagId",
					string(d.Id), "execTs", schedule, "err", qErr)
				return qErr
			}
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
		// Pop item from the queue
		_, pErr := queue.Pop()
		if pErr != nil && pErr != ds.ErrQueueIsEmpty {
			slog.Error("Cannot pop latest item from the queue", "err", pErr)
		}
		return uErr
	}
	// Upadate the next schedule for that DAG
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
func nextScheduleForDagRuns(
	ctx context.Context,
	dags []dag.Dag,
	currentTime time.Time,
	dbClient *db.Client,
) map[dag.Id]*time.Time {
	latestDagRunTime := make(map[dag.Id]*time.Time, len(dags))
	latestDagRuns, err := dbClient.ReadLatestDagRuns(ctx)
	if err != nil {
		slog.Error("Failed to load latest dag runs to create a cache. Cache is empty")
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
		if currentTime.After(nextSched) && !dag.Attr.CatchUp {
			// current time is after the supposed schedule and we don't want to catch up
			nextSchedFromCurrent := sched.Next(currentTime)
			latestDagRunTime[dag.Id] = &nextSchedFromCurrent
			continue
		}
		latestDagRunTime[dag.Id] = &nextSched
	}
	return latestDagRunTime
}
