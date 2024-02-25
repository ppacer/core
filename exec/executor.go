// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package exec

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/scheduler"
	"github.com/ppacer/core/timeutils"
)

type Executor struct {
	schedClient *scheduler.Client
	config      Config
	logDbClient *db.Client
}

// Executor configuration.
type Config struct {
	PollInterval       time.Duration
	HttpRequestTimeout time.Duration
}

// Setup default configuration values.
func defaultConfig() Config {
	return Config{
		PollInterval:       10 * time.Millisecond,
		HttpRequestTimeout: 30 * time.Second,
	}
}

// New creates new Executor instance. When config is nil, then default
// configuration values will be used.
func New(schedAddr string, logDbClient *db.Client, config *Config) *Executor {
	var cfg Config
	if config != nil {
		cfg = *config
	} else {
		cfg = defaultConfig()
	}
	httpClient := &http.Client{Timeout: cfg.HttpRequestTimeout}
	sc := scheduler.NewClient(schedAddr, httpClient, scheduler.DefaultClientConfig)
	return &Executor{
		schedClient: sc,
		config:      cfg,
		logDbClient: logDbClient,
	}
}

// Start starts executor. TODO...
func (e *Executor) Start(dags dag.Registry) {
	for {
		tte, err := e.schedClient.GetTask()
		if err == ds.ErrQueueIsEmpty {
			time.Sleep(e.config.PollInterval)
			continue
		}
		if err != nil {
			slog.Error("GetTask error", "err", err)
			break
		}
		slog.Info("Start executing task", "taskToExec", tte)
		d, dagExists := dags[dag.Id(tte.DagId)]
		if !dagExists {
			slog.Error("Could not get DAG from registry", "dagId", tte.DagId)
			continue
		}
		task, tErr := d.GetTask(tte.TaskId)
		if tErr != nil {
			slog.Error("Could not get task from DAG", "dagId", tte.DagId,
				"taskId", tte.TaskId)
			break
		}
		go executeTask(tte, task, e.schedClient, e.logDbClient)
	}
}

func executeTask(
	tte models.TaskToExec, task dag.Task, schedClient *scheduler.Client,
	logDbClient *db.Client,
) {
	defer func() {
		if r := recover(); r != nil {
			schedClient.UpsertTaskStatus(tte, dag.TaskFailed)
			slog.Error("Recovered from panic:", "err", r, "stack",
				string(debug.Stack()))
		}
	}()
	uErr := schedClient.UpsertTaskStatus(tte, dag.TaskRunning)
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			dag.TaskRunning.String(), "err", uErr.Error())
	}
	execTs, parseErr := timeutils.FromString(tte.ExecTs)
	if parseErr != nil {
		slog.Error("Error while parsing ExecTs", "ExecTs", tte.ExecTs, "err",
			parseErr.Error())
		schedClient.UpsertTaskStatus(tte, dag.TaskFailed)
		return
	}

	// Executing
	ri := dag.RunInfo{DagId: dag.Id(tte.DagId), ExecTs: execTs}
	taskContext := dag.TaskContext{
		Context: context.TODO(),
		Logger:  tasklog.NewSQLiteLogger(ri, tte.TaskId, logDbClient, nil),
		DagRun:  ri,
	}
	execErr := task.Execute(taskContext)
	if execErr != nil {
		slog.Error("Task finished with error", "tte", tte, "err",
			execErr.Error())
		schedClient.UpsertTaskStatus(tte, dag.TaskFailed)
		return
	}

	slog.Info("Finished executing task", "taskToExec", tte)
	uErr = schedClient.UpsertTaskStatus(tte, dag.TaskSuccess)
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			dag.TaskSuccess.String(), "err", uErr.Error())
	}
}
