// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

// Scheduler is the title object of this package, it connects all other
// components together. There should be single instance of a scheduler in
// single project - currently model of 1 scheduler and N executors is
// supported.
type Scheduler struct {
	dbClient       *db.Client
	config         Config
	queues         Queues
	logger         *slog.Logger
	notifier       notify.Sender
	goroutineCount int64

	sync.Mutex
	state State
}

// New returns new instance of Scheduler. Scheduler needs database client,
// queues for asynchronous communication and configuration. Database clients
// are by default available in db package (e.g. db.NewSqliteClient). Default
// configuration is set in DefaultConfig and default fixed-size buffer queues
// in DefaultQueues. In case when nil is provided as logger, then slog.Logger
// is instantiated with TextHandler and INFO severity level.
func New(dbClient *db.Client, queues Queues, config Config, logger *slog.Logger, notifier notify.Sender) *Scheduler {
	if logger == nil {
		opts := slog.HandlerOptions{Level: slog.LevelInfo}
		logger = slog.New(slog.NewTextHandler(os.Stdout, &opts))
	}
	return &Scheduler{
		dbClient:       dbClient,
		config:         config,
		queues:         queues,
		logger:         logger,
		notifier:       notifier,
		goroutineCount: 0,
		state:          StateStarted,
	}
}

// DefaultStarted creates default Scheduler using default configuration and
// SQLite databases, starts that scheduler and returns HTTP server with
// Scheduler endpoints. It's mainly to reduce boilerplate in simple examples
// and tests.
func DefaultStarted(dags dag.Registry, dbFile string, port int) *http.Server {
	logger := slog.Default()
	dbClient, dbErr := db.NewSqliteClient(dbFile, logger)
	if dbErr != nil {
		logger.Error("Cannot create Scheduler database client", "err",
			dbErr.Error())
		log.Panic(dbErr)
	}

	config := DefaultConfig
	notifier := notify.NewLogsErr(logger)
	sched := New(dbClient, DefaultQueues(config), config, logger, notifier)
	schedulerHttpHandler := sched.Start(dags)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: schedulerHttpHandler,
	}
	return server
}

// Start starts Scheduler. It synchronize internal queues with the database,
// fires up DAG watcher, task scheduler and finally returns HTTP ServeMux
// with attached HTTP endpoints for communication between scheduler and
// executors. TODO(dskrzypiec): more docs
func (s *Scheduler) Start(dags dag.Registry) http.Handler {
	cacheSize := s.config.DagRunTaskCacheLen
	taskCache := ds.NewLruCache[DRTBase, DagRunTaskState](cacheSize)

	// Setup timezone
	if s.config.TimezoneName != timeutils.LocalTimezoneName {
		tzSetErr := timeutils.SetTimezone(s.config.TimezoneName)
		if tzSetErr != nil {
			s.logger.Error(
				"Couldn't setup custom timezone. Local timezone will be used",
				"timezoneName", s.config.TimezoneName, "err", tzSetErr.Error(),
			)
		}
	}

	// Syncing queues with the database in case of program restarts.
	s.setState(StateSynchronizing)
	syncWithDatabase(dags, s.queues.DagRuns, s.dbClient, s.logger, s.config)
	cacheSyncErr := syncDagRunTaskCache(taskCache, s.dbClient, s.logger, s.config)
	if cacheSyncErr != nil {
		s.logger.Error("Cannot sync DAG run task cache", "err", cacheSyncErr)
		// There is no need to retry, it's just a cache.
	}

	dagRunWatcher := NewDagRunWatcher(
		s.queues.DagRuns, s.dbClient, s.logger, s.getState,
		s.config.DagRunWatcherConfig,
	)

	taskScheduler := NewTaskScheduler(
		dags, s.dbClient, s.queues, taskCache, s.config.TaskSchedulerConfig,
		s.logger, s.notifier, &s.goroutineCount, s.getState,
	)

	s.setState(StateRunning)
	atomic.AddInt64(&s.goroutineCount, 1)
	go func() {
		defer atomic.AddInt64(&s.goroutineCount, -1)
		// Running in the background dag run watcher
		dagRunWatcher.Watch(dags)
	}()

	go func() {
		defer atomic.AddInt64(&s.goroutineCount, -1)
		// Running in the background task scheduler
		taskScheduler.Start(dags)
	}()

	mux := http.NewServeMux()
	s.registerEndpoints(mux, taskScheduler)

	return mux
}

// Gets Scheduler current state.
func (s *Scheduler) getState() State {
	s.Lock()
	defer s.Unlock()
	return s.state
}

// Changes Scheduler state.
func (s *Scheduler) setState(newState State) {
	s.Lock()
	defer s.Unlock()
	s.state = newState
}

// Gets current number of goroutines spawned by Scheduler.
func (s *Scheduler) Goroutines() int64 {
	return atomic.LoadInt64(&s.goroutineCount)
}

// Register HTTP server endpoints for the Scheduler.
func (s *Scheduler) registerEndpoints(mux *http.ServeMux, ts *TaskScheduler) {
	mux.HandleFunc(getStateEndpoint, s.currentState)
	mux.HandleFunc(getTaskEndpoint, ts.popTask)
	mux.HandleFunc(upsertTaskStatusEndpoint, ts.upsertTaskStatus)
}

// HTTP handler for getting the current Scheduler State.
func (s *Scheduler) currentState(w http.ResponseWriter, _ *http.Request) {
	s.Lock()
	state := s.state
	s.Unlock()

	forJson := struct {
		StateString string `json:"state"`
	}{state.String()}

	stateJson, jsonErr := json.Marshal(forJson)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(stateJson))
}

// HTTP handler for popping dag run task from the queue.
func (ts *TaskScheduler) popTask(w http.ResponseWriter, _ *http.Request) {
	if ts.taskQueue.Size() == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	drt, err := ts.taskQueue.Pop()
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
		Retry:  drt.Retry,
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
		Retry:  drts.Retry,
	}

	ctx := context.TODO()
	updateErr := ts.UpsertTaskStatus(ctx, drt, status, drts.TaskErr)
	if updateErr != nil {
		msg := fmt.Sprintf("Error while updating dag run task status: %s",
			updateErr.Error())
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	ts.logger.Debug("Updated task status", "dagruntask", drt, "status", status,
		"duration", time.Since(start))
}

// State for representing current Scheduler state.
type State int

// Signature for function which return current Scheduler state.
type GetStateFunc func() State

const (
	StateStarted State = iota
	StateSynchronizing
	StateRunning
	StateStopping
	StateStopped
)

// String serializes State to its upper case string.
func (s State) String() string {
	return [...]string{
		"STARTED",
		"SYNCHRONIZING",
		"RUNNING",
		"STOPPING",
		"STOPPED",
	}[s]
}

// ParseState parses State based on given string. If that string does not match
// any State, then non-nil error is returned. State strings are case-sensitive.
func ParseState(s string) (State, error) {
	states := map[string]State{
		"STARTED":       StateStarted,
		"SYNCHRONIZING": StateSynchronizing,
		"RUNNING":       StateRunning,
		"STOPPING":      StateStopping,
		"STOPPED":       StateStopped,
	}
	if state, ok := states[s]; ok {
		return state, nil
	}
	return 0, fmt.Errorf("invalid Scheduler State: %s", s)
}
