// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

const defaultDbReadTimeout = 5 * time.Second

// FailedTaskManager covers complexity of the logic behind handling failed
// tasks. That includes a decision whenever a task should be retried and
// decision about sending external notification.
type failedTaskManager struct {
	dags      dag.Registry
	dbClient  *db.Client
	taskQueue ds.Queue[DagRunTask]
	taskCache ds.Cache[DRTBase, DagRunTaskState]
	config    TaskSchedulerConfig
	logger    *slog.Logger
}

// Initialize new failedTaskManager.
func newFailedTaskManager(
	dags dag.Registry,
	dbClient *db.Client,
	taskQueue ds.Queue[DagRunTask],
	taskCache ds.Cache[DRTBase, DagRunTaskState],
	config TaskSchedulerConfig,
	logger *slog.Logger,
) *failedTaskManager {
	return &failedTaskManager{
		dags:      dags,
		dbClient:  dbClient,
		taskQueue: taskQueue,
		taskCache: taskCache,
		config:    config,
		logger:    logger,
	}
}

// ShouldBeRetried checks whenever given DagRunTask should be retried based on
// its current status and its configuration. Additionally information about how
// long we should wait before scheduling another retry.
func (ftm *failedTaskManager) ShouldBeRetried(
	drt DagRunTask, status dag.TaskStatus,
) (bool, time.Duration, error) {
	if status != dag.TaskFailed {
		// Non-failed DAG run task shouldn't be retried.
		return false, 0, nil
	}

	drtNode, nodeErr := ftm.getDrtNode(drt)
	if nodeErr != nil {
		return false, 0, nodeErr
	}

	if drtNode.Config.Retries == 0 {
		// no retries!
		return false, 0, nil
	}
	delay := time.Duration(
		drtNode.Config.RetriesDelaySeconds * float64(time.Second),
	)

	// Check if the whole DAG run has been restarted - in that case we should
	// allow N * drt.Config.Retries retries.
	var N int = 0
	ctx, cancel := context.WithTimeout(context.Background(), defaultDbReadTimeout)
	defer cancel()
	dagRunLatestRestart, dbErr := ftm.dbClient.ReadDagRunRestartLatest(
		ctx, string(drt.DagId), timeutils.ToString(drt.AtTime),
	)
	if dbErr != nil && dbErr != sql.ErrNoRows {
		err := fmt.Errorf("cannot read latest DAG run restart for DagId=%s ExecTs=%s: %w",
			drt.DagId, timeutils.ToString(drt.AtTime), dbErr)
		return false, delay, err
	}
	if dbErr == nil {
		N = dagRunLatestRestart.Restart
	}

	// Check if we should retry the task.
	return drt.Retry < (N+1)*drtNode.Config.Retries, delay, nil
}

// CheckAndSendAlerts checks if in given situation external notification should
// be sent and if so, it will send it.
func (ftm *failedTaskManager) CheckAndSendAlerts(
	drt DagRunTask, status dag.TaskStatus, shouldBeRetried bool,
	taskErrStr *string, notifier notify.Sender,
) error {
	drtNode, nodeErr := ftm.getDrtNode(drt)
	if nodeErr != nil {
		ftm.logger.Error("Cannot get a node from the DAG", "drt", drt)
		return nodeErr
	}

	var taskErr error = nil
	if taskErrStr == nil {
		ftm.logger.Error("Got failed task with empty task error",
			"dagruntask", drt, "status", status.String())
	} else {
		taskErr = fmt.Errorf("%s", *taskErrStr)
	}

	if drtNode.Config.Notifier != nil {
		ftm.logger.Debug("Custom notifier will be used for the task", "drt", drt)
		notifier = drtNode.Config.Notifier
	}

	ctx := context.TODO()
	msg := notify.MsgData{
		DagId:        string(drt.DagId),
		ExecTs:       timeutils.ToString(drt.AtTime),
		TaskId:       &drt.TaskId,
		TaskRunError: taskErr,
	}

	if shouldBeRetried && drtNode.Config.SendAlertOnRetry {
		return notifier.Send(ctx, drtNode.Config.AlertOnRetryTemplate, msg)
	}

	if !shouldBeRetried && drtNode.Config.SendAlertOnFailure {
		return notifier.Send(ctx, drtNode.Config.AlertOnFailureTemplate, msg)
	}
	return nil
}

func (ftm *failedTaskManager) getDrtNode(drt DagRunTask) (*dag.Node, error) {
	dag, exists := ftm.dags[drt.DagId]
	if !exists {
		return nil, fmt.Errorf("dag [%s] does not exist in the registry",
			string(drt.DagId))
	}
	drtNode, nErr := dag.GetNode(drt.TaskId)
	if nErr != nil {
		return drtNode, nErr
	}
	if drtNode == nil {
		return nil,
			fmt.Errorf("expected non-nil dag.Node for DAG=%s and Task=%s",
				drt.DagId, drt.TaskId)
	}
	return drtNode, nil
}
