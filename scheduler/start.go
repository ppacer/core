// Package scheduler provides functions for creating and starting new
// Scheduler.
//
// TODO more docs and examples.
package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/timeutils"
)

// This function is called on scheduler start up. TODO: more docs.
func syncWithDatabase(
	queue ds.Queue[DagRun], dbClient *db.Client, config Config,
) {
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		config.StartupContextTimeout,
		errors.New("scheduler initial sync timeouted"),
	)
	defer cancel()
	dagTasksSyncErr := syncDags(ctx, dbClient)
	if dagTasksSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		slog.Error("Cannot sync up dag.registry and dagtasks", "err",
			dagTasksSyncErr)
	}
	queueSyncErr := syncDagRunsQueue(
		ctx, queue, dbClient, config.DagRunWatcherConfig,
	)
	if queueSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		slog.Error("Cannot sync up dag runs queue", "err", queueSyncErr)
	}
}

// Synchronize all DAGs from dag.registry with dags and dagtasks tables in the
// database. If only DAG attributes were changed then the record in dags table
// would be updated and dagtasks would not.
func syncDags(ctx context.Context, dbClient *db.Client) error {
	start := time.Now()
	slog.Info("Start syncing dag.registry with dags and dagtasks tables")
	for _, d := range dag.List() {
		select {
		case <-ctx.Done():
			// TODO(dskrzypiec): what now? Probably retries... and eventually
			// panic
			slog.Error("Context for Scheduler initial sync timeouted", "err",
				ctx.Err())
		default:
		}
		syncErr := syncDag(ctx, dbClient, d)
		if syncErr != nil {
			// TODO(dskrzypiec): Probably we should retunr []error and don't
			// stop when syncing one DAG would fail This should be solved
			// together with general approach to error handling in the
			// scheduler.
			return syncErr
		}
	}
	slog.Info("Finished syncing dag.Registry with dags and dagtasks tables",
		"duration", time.Since(start))
	return nil
}

// Synchronize given DAG with dags and dagtasks tables in the database.
func syncDag(ctx context.Context, dbClient *db.Client, d dag.Dag) error {
	dagId := string(d.Id)
	dbDag, dbRErr := dbClient.ReadDag(ctx, dagId)
	if dbRErr != nil && dbRErr != sql.ErrNoRows {
		slog.Error("Could not get Dag metadata from dags table", "dagId", dagId,
			"err", dbRErr)
		return dbRErr
	}
	if dbRErr == sql.ErrNoRows {
		// There is no DAG in the database yet
		uErr := dbClient.UpsertDag(ctx, d)
		if uErr != nil {
			slog.Error("Could not insert DAG into dags table", "dagId", dagId,
				"err", uErr)
			return uErr
		}
		dtErr := dbClient.InsertDagTasks(ctx, d)
		if dtErr != nil {
			slog.Error("Could not insert DAG tasks into dagtasks table",
				"dagId", dagId, "err", dtErr)
			return dtErr
		}
		return nil
	}
	// Check if update is needed
	if dbDag.HashDagMeta == d.HashDagMeta() && dbDag.HashTasks == d.HashTasks() {
		slog.Debug("There is no need for update. HashAttr and HashTasks are "+
			"not changed", "dagId", dagId)
		return nil
	}
	// Either attributes or tasks for this DAG were modified
	uErr := dbClient.UpsertDag(ctx, d)
	if uErr != nil {
		slog.Error("Could not update DAG in dags table", "dagId", dagId, "err",
			uErr)
		return uErr
	}
	if dbDag.HashTasks != d.HashTasks() {
		slog.Info("Hash tasks has changed. New version of dagtasks will be inserted.",
			"dagId", dagId, "prevHashTasks", dbDag.HashTasks, "currHashTasks",
			d.HashTasks())
		dtErr := dbClient.InsertDagTasks(ctx, d)
		if dtErr != nil {
			slog.Error("Could not insert DAG tasks into dagtasks table", "dagId",
				dagId)
			return dtErr
		}
	}
	return nil
}

// Synchronize dag runs with statuses SCHEDULED and READY_TO_SCHEDULE in the
// database, to be put on the queue. This might happen when dag runs were
// scheduled but before they started to run scheduler restarted or something
// else happened.
func syncDagRunsQueue(
	ctx context.Context,
	q ds.Queue[DagRun],
	dbClient *db.Client,
	config DagRunWatcherConfig,
) error {
	dagrunsToSchedule, dbErr := dbClient.ReadDagRunsNotFinished(ctx)
	if dbErr != nil {
		return dbErr
	}
	for _, dr := range dagrunsToSchedule {
		for q.Capacity() <= 0 {
			slog.Warn("The dag run queue is full. Will try in moment",
				"QueueIsFullInterval", config.QueueIsFullInterval)
			time.Sleep(config.QueueIsFullInterval)
		}
		q.Put(DagRun{
			DagId:  dag.Id(dr.DagId),
			AtTime: timeutils.FromStringMust(dr.ExecTs),
		})
	}
	return nil
}

// TODO
func syncDagRunTaskCache(
	ctx context.Context,
	cache ds.Cache[DagRunTask, DagRunTaskState],
	dbClient *db.Client,
) error {
	// TODO: We want to sync DagRunTasks which are not in terminal states
	// (failed, success)
	slog.Error("TODO: syncDagRunTaskCache is not yet implemented!")
	return nil
}
