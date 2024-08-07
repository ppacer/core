// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

/*
Package exec defines ppacer Executor and related functionalities.

Executor on start up (Start method) spins endless loop in which it polls
Scheduler (via HTTP), to check if there are new tasks, to be executed. Polling
is performed according to provided strategy (pace.Strategy). New tasks are
executed in separate goroutines. When executed task receive runtime error,
Executor will recover and will mark task execution as failed.

Executor might be used in the same program as Scheduler (in a separate
goroutine) or can be put in a separate binary. Also there can be many
executors, potentially on multiple machines.
*/
package exec

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/pace"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

// Executor executes DAG run tasks, each in a separate goroutine. When Start
// method is called Executor starts polling ppacer scheduler for new DAG run
// tasks to be run.
type Executor struct {
	schedClient     *scheduler.Client
	pollingStrategy pace.Strategy
	config          Config
	taskLogs        tasklog.Factory
	logger          *slog.Logger
	notifier        notify.Sender
	goroutineCount  *int64
}

// Executor configuration.
type Config struct {
	HttpRequestTimeout time.Duration
	MaxGoroutineCount  int64
}

// Setup default configuration values.
func defaultConfig() Config {
	return Config{
		HttpRequestTimeout: 30 * time.Second,
		MaxGoroutineCount:  1000,
	}
}

// New creates new Executor instance. When polling strategy is nil, then linear
// backoff (min=1ms, max=1s, step=10ms, repeat=10) will be sued. When config is
// nil, then default configuration values will be used. When logger is nil,
// then slog for stdout with WARN severity level will be used. When notifier is
// nil, then notify.NewLogsErr (notifications as logs) will be used.
func New(
	schedAddr string,
	logDbClient *db.LogsClient,
	polling pace.Strategy,
	logger *slog.Logger,
	config *Config,
	notifier notify.Sender,
) *Executor {
	var cfg Config
	if config != nil {
		cfg = *config
	} else {
		cfg = defaultConfig()
	}
	if polling == nil {
		polling, _ = pace.NewLinearBackoff(
			1*time.Millisecond,
			1*time.Second,
			10*time.Millisecond,
			10,
		)
	}
	if logger == nil {
		opts := slog.HandlerOptions{Level: slog.LevelWarn}
		logger = slog.New(slog.NewTextHandler(os.Stdout, &opts))
	}
	if notifier == nil {
		notifier = notify.NewLogsErr(logger)
	}
	httpClient := &http.Client{Timeout: cfg.HttpRequestTimeout}
	scfg := scheduler.DefaultClientConfig
	sc := scheduler.NewClient(schedAddr, httpClient, logger, scfg)
	var goroutineCount int64 = 0
	return &Executor{
		schedClient:     sc,
		pollingStrategy: polling,
		config:          cfg,
		taskLogs:        tasklog.NewSQLite(logDbClient, nil),
		logger:          logger,
		notifier:        notifier,
		goroutineCount:  &goroutineCount,
	}
}

// NewDefault creates new Executor using SQLite for task logs and default
// configuration for Executor. It's mainly to reduce boilerplate in simple
// examples and tests.
func NewDefault(schedulerUrl, taskLogsDbFile string) *Executor {
	logger := slog.Default()
	logsDbClient, logsDbErr := db.NewSqliteClientForLogs(taskLogsDbFile, logger)
	if logsDbErr != nil {
		logger.Error("Cannot create SQLite database for task logs", "err",
			logsDbErr.Error())
		log.Panic(logsDbErr)
	}
	return New(schedulerUrl, logsDbClient, nil, nil, nil, nil)
}

// Start starts executor main loop. It polls, according to the polling
// strategy, Scheduler for new tasks to be executed. Tasks are executed in
// separate goroutines. There's a limit for number of goroutines running at the
// same time (default is 1000). If that limit is hit, Executor would wait with
// starting execution new task, until number of currently running goroutines
// would go below the limit.
func (e *Executor) Start(dags dag.Registry) {
	for {
		tte, err := e.schedClient.GetTask()
		if err == ds.ErrQueueIsEmpty || errors.Is(err, syscall.ECONNREFUSED) {
			time.Sleep(e.pollingStrategy.NextInterval())
			continue
		}
		if err != nil {
			e.logger.Error("GetTask error", "err", err)
			break
		}
		e.pollingStrategy.Reset()
		e.logger.Info("Start executing task", "taskToExec", tte)
		d, dagExists := dags[dag.Id(tte.DagId)]
		if !dagExists {
			e.logger.Error("Could not get DAG from registry", "dagId", tte.DagId)
			continue
		}
		taskNode, tErr := d.GetNode(tte.TaskId)
		if tErr != nil {
			e.logger.Error("Could not get task node from DAG", "dagId",
				tte.DagId, "taskId", tte.TaskId)
			break
		}
		if taskNode == nil {
			e.logger.Error("Node is nil", "tte", tte)
			break
		}
		notifier := e.notifier
		if taskNode.Config.Notifier != nil {
			notifier = taskNode.Config.Notifier
		}
		e.waitIfCannotSpawnNewGoroutine()
		atomic.AddInt64(e.goroutineCount, 1)
		go executeTask(tte, taskNode, e.schedClient, e.taskLogs, e.logger,
			notifier, e.goroutineCount)
	}
}

func executeTask(
	tte api.TaskToExec, node *dag.Node, schedClient *scheduler.Client,
	taskLogs tasklog.Factory, logger *slog.Logger, notifier notify.Sender,
	goroutineCount *int64,
) {
	defer atomic.AddInt64(goroutineCount, -1)
	uErr := schedClient.UpsertTaskStatus(tte, dag.TaskRunning, nil)
	if uErr != nil {
		logger.Error("Error while updating status", "tte", tte, "status",
			dag.TaskRunning.String(), "err", uErr.Error())
	}
	execTs, parseErr := timeutils.FromString(tte.ExecTs)
	if parseErr != nil {
		logger.Error("Error while parsing ExecTs", "ExecTs", tte.ExecTs, "err",
			parseErr.Error())
		schedClient.UpsertTaskStatus(tte, dag.TaskFailed, parseErr)
		return
	}

	ri := dag.RunInfo{DagId: dag.Id(tte.DagId), ExecTs: execTs}
	ti := tasklog.TaskInfo{
		DagId:  tte.DagId,
		ExecTs: execTs,
		TaskId: node.Task.Id(),
		Retry:  tte.Retry,
	}
	taskLogger := taskLogs.GetLogger(ti)

	timeout := time.Duration(node.Config.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	taskContext := dag.TaskContext{
		Context:  ctx,
		Logger:   taskLogger,
		DagRun:   ri,
		Notifier: notifier,
	}

	// Executing
	done := make(chan error, 1)
	atomic.AddInt64(goroutineCount, 1)
	go func() {
		defer recoverTaskRuntimeErr(schedClient, logger, tte, taskLogger)
		defer atomic.AddInt64(goroutineCount, -1)
		done <- node.Task.Execute(taskContext)
	}()

	select {
	case <-ctx.Done():
		taskLogger.Error("Task execution exceeded timeout", "timeout", timeout)
		logger.Error("Task execution exceeded timeout", "tte", tte, "timeout",
			timeout)
		tuErr := schedClient.UpsertTaskStatus(
			tte, dag.TaskFailed,
			fmt.Errorf("task execution exceeded timeout of %v", timeout),
		)
		if tuErr != nil {
			logger.Error("Error while updating status", "tte", tte, "status",
				dag.TaskFailed.String(), "err", tuErr.Error())
		}

	case execErr := <-done:
		if execErr != nil {
			logger.Error("Task finished with error", "tte", tte, "err",
				execErr.Error())
			tuErr := schedClient.UpsertTaskStatus(tte, dag.TaskFailed, execErr)
			if tuErr != nil {
				logger.Error("Error while updating status", "tte", tte,
					"status", dag.TaskFailed.String(), "err", uErr.Error(),
				)
			}
			return
		}

		logger.Info("Finished executing task", "taskToExec", tte)
		uErr = schedClient.UpsertTaskStatus(tte, dag.TaskSuccess, nil)
		if uErr != nil {
			logger.Error("Error while updating status", "tte", tte, "status",
				dag.TaskSuccess.String(), "err", uErr.Error())
		}
	}
}

func recoverTaskRuntimeErr(
	schedClient *scheduler.Client, logger *slog.Logger, tte api.TaskToExec,
	taskLogger *slog.Logger,
) {
	if r := recover(); r != nil {
		stackStr := string(debug.Stack())
		stackAsErr := fmt.Errorf("%s", stackStr)
		schedClient.UpsertTaskStatus(tte, dag.TaskFailed, stackAsErr)
		logger.Error("Recovered from panic", "tte", tte, "err", r, "stack",
			string(debug.Stack()))

		taskLogger.Error(
			"Runtime error for taks execution", "taskToExec", tte, "stackTrace",
			stackStr)
	}
}

// waitIfCannotSpawnNewGoroutine check whenever new goroutine could be started,
// based on maxGoroutineCount configuration. If that cannot be done, this
// method would block until number of goroutines is below the limit.
func (e *Executor) waitIfCannotSpawnNewGoroutine() {
	prevTs := time.Now()
	for {
		if atomic.LoadInt64(e.goroutineCount) < int64(e.config.MaxGoroutineCount) {
			return
		}
		now := time.Now()
		if now.Sub(prevTs) > 30*time.Second {
			e.logger.Warn("Cannot yet start new goroutine, Executor hit the limit.",
				"limit", e.config.MaxGoroutineCount)
			prevTs = now
		}
	}
}
