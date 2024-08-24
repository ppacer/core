// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

// Scheduler is the title object of this package, it connects all other
// components together. There should be single instance of a scheduler in
// single project - currently model of 1 scheduler and N executors is
// supported.
type Scheduler struct {
	dbClient      *db.Client
	taskLogs      tasklog.Factory
	config        Config
	queues        Queues
	logger        *slog.Logger
	notifier      notify.Sender
	taskScheduler *TaskScheduler

	sync.Mutex
	state State
}

// New returns new instance of Scheduler. Scheduler needs database client,
// queues for asynchronous communication and configuration. Database clients
// are by default available in db package (e.g. db.NewSqliteClient). Default
// configuration is set in DefaultConfig and default fixed-size buffer queues
// in DefaultQueues. In case when nil is provided as logger, then slog.Logger
// is instantiated with TextHandler and INFO severity level (unless
// PPACER_LOG_LEVEL env viariable is set).
func New(
	dbClient *db.Client,
	taskLogs tasklog.Factory,
	queues Queues,
	config Config,
	logger *slog.Logger,
	notifier notify.Sender,
) *Scheduler {
	if logger == nil {
		logger = defaultLogger()
	}
	return &Scheduler{
		dbClient: dbClient,
		taskLogs: taskLogs,
		config:   config,
		queues:   queues,
		logger:   logger,
		notifier: notifier,
		state:    StateStarted,
	}
}

// DefaultStarted creates default Scheduler using default configuration and
// SQLite databases, starts that scheduler and returns HTTP server with
// Scheduler endpoints. It's mainly to reduce boilerplate in simple examples
// and tests.
func DefaultStarted(
	ctx context.Context, dags dag.Registry, dbFile, dbLogsFile string, port int,
) *http.Server {
	logger := defaultLogger()
	dbClient, dbErr := db.NewSqliteClient(dbFile, logger)
	if dbErr != nil {
		logger.Error("Cannot create Scheduler database client", "err",
			dbErr.Error())
		log.Panic(dbErr)
	}
	dbLogsClient, dbLogsErr := db.NewSqliteClientForLogs(dbLogsFile, logger)
	if dbLogsErr != nil {
		logger.Error("Cannot create task logs database client", "err",
			dbLogsErr.Error())
		log.Panic(dbLogsErr)
	}
	taskLogs := tasklog.NewSQLite(dbLogsClient, nil)

	config := DefaultConfig
	notifier := notify.NewLogsErr(logger)
	sched := New(
		dbClient, taskLogs, DefaultQueues(config), config, logger,
		notifier,
	)
	schedulerHttpHandler := sched.Start(ctx, dags)
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
func (s *Scheduler) Start(ctx context.Context, dags dag.Registry) http.Handler {
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
	cacheSyncErr := syncDagRunTaskCache(
		ctx, taskCache, s.dbClient, s.logger, s.config,
	)
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
		s.logger, s.notifier, 0, s.getState,
	)
	s.taskScheduler = taskScheduler

	s.setState(StateRunning)
	go func() {
		// Running in the background dag run watcher
		dagRunWatcher.Watch(ctx, dags)
	}()

	go func() {
		// Running in the background task scheduler
		taskScheduler.Start(ctx, dags)
	}()

	go func() {
		<-ctx.Done()
		s.logger.Info("Scheduler is about to be shut down. Context is done.",
			"ctxErr", ctx.Err().Error())
		s.setState(StateStopping)
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

// Gets current number of goroutines spawned by (Task)Scheduler. It excludes
// long-running goroutines spawned by Scheduler.Start and focuses on
// goroutines from TaskScheduler where almost the all pressure is.
func (s *Scheduler) Goroutines() int {
	return s.taskScheduler.goroutines()
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
