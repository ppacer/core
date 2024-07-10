// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/notify"
	"github.com/ppacer/core/timeutils"
)

// TODO
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

// ShouldBeRetried checks
func (ftm *failedTaskManager) ShouldBeRetried(
	drt DagRunTask, status dag.TaskStatus,
) (bool, error) {
	if status != dag.TaskFailed {
		// Non-failed DAG run task shouldn't be retried.
		return false, nil
	}

	drtNode, nodeErr := ftm.getDrtNode(drt)
	if nodeErr != nil {
		return false, nodeErr
	}

	if drtNode.Config.Retries == 0 {
		// no retries!
		return false, nil
	}
	return drt.Retry < drtNode.Config.Retries, nil
}

// CheckAndSendAlerts checks if in given situation external notification should
// be sent and if so, it will send it.
func (ftm *failedTaskManager) CheckAndSendAlerts(
	drt DagRunTask, status dag.TaskStatus, shouldBeRetried bool,
	taskErrStr *string, notifier notify.Sender,
) error {
	drtNode, nodeErr := ftm.getDrtNode(drt)
	if nodeErr != nil {
		return nodeErr
	}

	var taskErr error = nil
	if taskErrStr == nil {
		ftm.logger.Error("Got failed task with empty task error",
			"dagruntask", drt, "status", status.String())
	} else {
		taskErr = fmt.Errorf("%s", *taskErrStr)
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

	if drtNode.Config.SendAlertOnFailure {
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
