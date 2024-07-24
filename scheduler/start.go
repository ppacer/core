// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

/*
Package scheduler provides functionality for creating and starting new Scheduler.

# Introduction

Scheduler type is the hearth of ppacer. Once initialized and started it does
the following steps:
  - Setup internal caches and configuration (eg timezone).
  - Synchronize metadata on current dag.Registry with the database.
  - Setup new DagRunWatcher, to listen on schedules and starts new DAG runs.
  - Setup new TaskScheduler, to listen on new DAG runs and start scheduling its
    tasks.
  - Register Scheduler endpoints in form of http.Handler.

It's meant to have single instance of Scheduler per program. Types
DagRunWatcher and TaskScheduler are public primarily due to exposing
documentation and they usually shouldn't be accessed directly. It's enough, to
have an instance of Scheduler.

# Default Scheduler

The simplest way, to start new Scheduler is using DefaultStarted function.
Let's take a look on the following example:

	ctx := context.TODO()
	dags := dag.Registry{} // add your dags or use existing Registry
	schedulerHandler := DefaultStarted(ctx, dags, "scheduler.db", 9191)
	lasErr := schedulerHandler.ListenAndServe()
	if lasErr != nil {
		log.Panicf("Cannot start the server: %s\n", lasErr.Error())
	}

Function DefaultStarted starts Scheduler with default configuration, including
notifier in form of log ERR messages, SQLite database and default slog.Logger.

To get full example of ppacer "hello world", please refer to:
https://ppacer.org/start/intro/
*/
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
	dags dag.Registry, queue ds.Queue[DagRun], dbClient *db.Client,
	logger *slog.Logger, config Config,
) {
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		config.StartupContextTimeout,
		errors.New("scheduler initial sync timeouted"),
	)
	defer cancel()
	dagTasksSyncErr := syncDags(ctx, dags, dbClient, logger)
	if dagTasksSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		logger.Error("Cannot sync up dags and dagtasks", "err",
			dagTasksSyncErr)
	}
	queueSyncErr := syncDagRunsQueue(
		ctx, queue, dbClient, logger, config.DagRunWatcherConfig,
	)
	if queueSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		logger.Error("Cannot sync up dag runs queue", "err", queueSyncErr)
	}
}

// Synchronize all DAGs from given registry with dags and dagtasks tables in
// the database. If only DAG attributes were changed then the record in dags
// table would be updated and dagtasks would not.
func syncDags(ctx context.Context, dags dag.Registry, dbClient *db.Client, logger *slog.Logger) error {
	start := time.Now()
	logger.Info("Start syncing DAG registry with dags and dagtasks tables")
	for _, d := range dags {
		select {
		case <-ctx.Done():
			// TODO(dskrzypiec): what now? Probably retries... and eventually
			// panic
			logger.Error("Context for Scheduler initial sync timeouted", "err",
				ctx.Err())
		default:
		}
		syncErr := syncDag(ctx, dbClient, d, logger)
		if syncErr != nil {
			// TODO(dskrzypiec): Probably we should retunr []error and don't
			// stop when syncing one DAG would fail This should be solved
			// together with general approach to error handling in the
			// scheduler.
			return syncErr
		}
	}
	logger.Info("Finished syncing DAG registry with dags and dagtasks tables",
		"duration", time.Since(start))
	return nil
}

// Synchronize given DAG with dags and dagtasks tables in the database.
func syncDag(ctx context.Context, dbClient *db.Client, d dag.Dag, logger *slog.Logger) error {
	dagId := string(d.Id)
	dbDag, dbRErr := dbClient.ReadDag(ctx, dagId)
	if dbRErr != nil && dbRErr != sql.ErrNoRows {
		logger.Error("Could not get Dag metadata from dags table", "dagId", dagId,
			"err", dbRErr)
		return dbRErr
	}
	if dbRErr == sql.ErrNoRows {
		// There is no DAG in the database yet
		uErr := dbClient.UpsertDag(ctx, d)
		if uErr != nil {
			logger.Error("Could not insert DAG into dags table", "dagId", dagId,
				"err", uErr)
			return uErr
		}
		dtErr := dbClient.InsertDagTasks(ctx, d)
		if dtErr != nil {
			logger.Error("Could not insert DAG tasks into dagtasks table",
				"dagId", dagId, "err", dtErr)
			return dtErr
		}
		return nil
	}
	// Check if update is needed
	if dbDag.HashDagMeta == d.HashDagMeta() && dbDag.HashTasks == d.HashTasks() {
		logger.Debug("There is no need for update. HashAttr and HashTasks are "+
			"not changed", "dagId", dagId)
		return nil
	}
	// Either attributes or tasks for this DAG were modified
	uErr := dbClient.UpsertDag(ctx, d)
	if uErr != nil {
		logger.Error("Could not update DAG in dags table", "dagId", dagId, "err",
			uErr)
		return uErr
	}
	if dbDag.HashTasks != d.HashTasks() {
		logger.Info("Hash tasks has changed. New version of dagtasks will be inserted.",
			"dagId", dagId, "prevHashTasks", dbDag.HashTasks, "currHashTasks",
			d.HashTasks())
		dtErr := dbClient.InsertDagTasks(ctx, d)
		if dtErr != nil {
			logger.Error("Could not insert DAG tasks into dagtasks table",
				"dagId", dagId)
			return dtErr
		}
	}
	return nil
}

// Synchronize not completed dag runs in the database, to be put on the queue.
// This might happen when dag runs were scheduled but before they started to
// run scheduler restarted or something else happened.
func syncDagRunsQueue(
	ctx context.Context,
	q ds.Queue[DagRun],
	dbClient *db.Client,
	logger *slog.Logger,
	config DagRunWatcherConfig,
) error {
	dagrunsToSchedule, dbErr := dbClient.ReadDagRunsNotFinished(ctx)
	if dbErr != nil {
		return dbErr
	}
	for _, dr := range dagrunsToSchedule {
		for q.Capacity() <= 0 {
			logger.Warn("The dag run queue is full. Will try in moment",
				"QueueIsFullInterval", config.QueueIsFullInterval)
			time.Sleep(config.QueueIsFullInterval)
			// TODO: this might blocks forever. Currently sync is done at the
			// startup when DagRunWatcher is not yet started, so DagRun cannot
			// be consumed from that queue. In another words, currently we can
			// only sync up to DagRun queue length of not finished DagRuns.
			// That should be rather rare case, but we need to fix it!
		}
		q.Put(DagRun{
			DagId:  dag.Id(dr.DagId),
			AtTime: timeutils.FromStringMust(dr.ExecTs),
		})
	}
	return nil
}

// Synchronize not finished DAG run tasks based on database table. Those DAG
// run tasks and their statuses are put into a cache. If number of unfinished
// tasks are greater than size of the cache, then older tasks won't fit into
// the cache. Newer tasks are more relevant.
func syncDagRunTaskCache(
	ctx context.Context,
	cache ds.Cache[DRTBase, DagRunTaskState],
	dbClient *db.Client,
	logger *slog.Logger,
	config Config,
) error {
	dbCtx, cancel := context.WithTimeout(ctx, config.StartupContextTimeout)
	defer cancel()
	drtNotFinished, dbErr := dbClient.ReadDagRunTasksNotFinished(dbCtx)
	if dbErr != nil {
		return dbErr
	}
	for _, drtDb := range drtNotFinished {
		drt := DRTBase{
			DagId:  dag.Id(drtDb.DagId),
			AtTime: timeutils.FromStringMust(drtDb.ExecTs),
			TaskId: drtDb.TaskId,
		}
		status, sErr := dag.ParseTaskStatus(drtDb.Status)
		if sErr != nil {
			logger.Error("Cannot parse dag run task status from string from dagruntasks table",
				"status", drtDb.Status, "err", sErr.Error())
			continue
		}
		drts := DagRunTaskState{
			Status:         status,
			StatusUpdateTs: timeutils.FromStringMust(drtDb.StatusUpdateTs),
		}
		cache.Put(drt, drts)
	}
	return nil
}
