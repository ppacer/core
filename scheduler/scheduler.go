// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/timeutils"
)

// Scheduler is the title object of this package, it connects all other
// components together. There should be single instance of a scheduler in
// single project - currently model of 1 scheduler and N executors are
// supported.
type Scheduler struct {
	dbClient *db.Client
	config   Config
	queues   Queues
}

// New returns new instance of Scheduler. Scheduler needs database client,
// queues for asynchronous communication and configuration. Database clients
// are by default available in db package (e.g. db.NewSqliteClient). Default
// configuration is set in DefaultConfig and default fixed-size buffer queues
// in DefaultQueues.
func New(dbClient *db.Client, queues Queues, config Config) *Scheduler {
	return &Scheduler{
		dbClient: dbClient,
		config:   config,
		queues:   queues,
	}
}

// Start starts Scheduler. It synchronize internal queues with the database,
// fires up DAG watcher, task scheduler and finally returns HTTP ServeMux
// with attached HTTP endpoints for communication between scheduler and
// executors. TODO(dskrzypiec): more docs
func (s *Scheduler) Start(dags dag.Registry) http.Handler {
	cacheSize := s.config.DagRunTaskCacheLen
	taskCache := ds.NewLruCache[DagRunTask, DagRunTaskState](cacheSize)

	// Syncing queues with the database in case of program restarts.
	syncWithDatabase(dags, s.queues.DagRuns, s.dbClient, s.config)
	cacheSyncErr := syncDagRunTaskCache(taskCache, s.dbClient, s.config)
	if cacheSyncErr != nil {
		slog.Error("Cannot sync DAG run task cache", "err", cacheSyncErr)
		// There is no need to retry, it's just a cache.
	}

	dagRunWatcher := NewDagRunWatcher(
		s.queues.DagRuns, s.dbClient, s.config.DagRunWatcherConfig,
	)

	taskScheduler := TaskScheduler{
		DbClient:    s.dbClient,
		DagRunQueue: s.queues.DagRuns,
		TaskQueue:   s.queues.DagRunTasks,
		TaskCache:   taskCache,
		Config:      s.config.TaskSchedulerConfig,
	}

	go func() {
		// Running in the background dag run watcher
		// TODO(dskrzypiec): Probably move it as Start parameter (dags
		// []dag.Dag).
		dagRunWatcher.Watch(dags)
	}()

	go func() {
		// Running in the background task scheduler
		taskScheduler.Start(dags)
	}()

	mux := http.NewServeMux()
	s.registerEndpoints(mux, &taskScheduler)

	return mux
}

func (s *Scheduler) registerEndpoints(mux *http.ServeMux, ts *TaskScheduler) {
	mux.HandleFunc(getTaskEndpoint, ts.popTask)
	mux.HandleFunc(upsertTaskStatusEndpoint, ts.upsertTaskStatus)
}

// HTTP handler for popping dag run task from the queue.
func (ts *TaskScheduler) popTask(w http.ResponseWriter, _ *http.Request) {
	if ts.TaskQueue.Size() == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	drt, err := ts.TaskQueue.Pop()
	if err == ds.ErrQueueIsEmpty {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		errMsg := fmt.Sprintf("cannot get scheduled task from the queue: %s",
			err.Error())
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	drtmodel := models.TaskToExec{
		DagId:  string(drt.DagId),
		ExecTs: timeutils.ToString(drt.AtTime),
		TaskId: drt.TaskId,
	}
	jsonBytes, jsonErr := json.Marshal(drtmodel)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}

// Updates task status in the task cache and the database.
func (ts *TaskScheduler) upsertTaskStatus(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != "POST" {
		http.Error(w, "Only POST requests are allowed",
			http.StatusMethodNotAllowed)
		return
	}

	var drts models.DagRunTaskStatus
	err := json.NewDecoder(r.Body).Decode(&drts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	execTs, tErr := timeutils.FromString(drts.ExecTs)
	if tErr != nil {
		msg := fmt.Sprintf("Given execTs timestamp in incorrect format: %s",
			tErr.Error())
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	status, statusErr := dag.ParseTaskStatus(drts.Status)
	if statusErr != nil {
		msg := fmt.Sprintf("Incorrect dag run task status: %s",
			statusErr.Error())
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	drt := DagRunTask{
		DagId:  dag.Id(drts.DagId),
		AtTime: execTs,
		TaskId: drts.TaskId,
	}

	ctx := context.TODO()
	updateErr := ts.UpsertTaskStatus(ctx, drt, status)
	if updateErr != nil {
		msg := fmt.Sprintf("Error while updating dag run task status: %s",
			updateErr.Error())
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	slog.Debug("Updated task status", "dagruntask", drt, "status", status,
		"duration", time.Since(start))
}
