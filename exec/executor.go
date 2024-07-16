// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package exec defines ppacer Executor and related functionalities.
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

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
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
func New(schedAddr string, logDbClient *db.Client, polling pace.Strategy, logger *slog.Logger, config *Config, notifier notify.Sender) *Executor {
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

// Start starts executor. TODO...
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
		notifier := e.notifier
		if taskNode.Config.Notifier != nil {
			notifier = taskNode.Config.Notifier
		}
		e.waitIfCannotSpawnNewGoroutine()
		atomic.AddInt64(e.goroutineCount, 1)
		go executeTask(tte, taskNode.Task, e.schedClient, e.taskLogs, e.logger,
			notifier, e.goroutineCount)
	}
}

func executeTask(
	tte models.TaskToExec, task dag.Task, schedClient *scheduler.Client,
	taskLogs tasklog.Factory, logger *slog.Logger, notifier notify.Sender,
	goroutineCount *int64,
) {
	defer func() {
		if r := recover(); r != nil {
			stackAsErr := fmt.Errorf("%s", string(debug.Stack()))
			schedClient.UpsertTaskStatus(tte, dag.TaskFailed, stackAsErr)
			logger.Error("Recovered from panic:", "err", r, "stack",
				string(debug.Stack()))
		}
	}()
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

	// Executing
	ri := dag.RunInfo{DagId: dag.Id(tte.DagId), ExecTs: execTs}
	ti := tasklog.TaskInfo{
		DagId:  tte.DagId,
		ExecTs: execTs,
		TaskId: task.Id(),
		Retry:  tte.Retry,
	}

	taskContext := dag.TaskContext{
		Context:  context.TODO(),
		Logger:   taskLogs.GetLogger(ti),
		DagRun:   ri,
		Notifier: notifier,
	}
	execErr := task.Execute(taskContext)
	if execErr != nil {
		logger.Error("Task finished with error", "tte", tte, "err",
			execErr.Error())
		schedClient.UpsertTaskStatus(tte, dag.TaskFailed, execErr)
		return
	}

	logger.Info("Finished executing task", "taskToExec", tte)
	uErr = schedClient.UpsertTaskStatus(tte, dag.TaskSuccess, nil)
	if uErr != nil {
		logger.Error("Error while updating status", "tte", tte, "status",
			dag.TaskSuccess.String(), "err", uErr.Error())
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
