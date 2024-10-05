// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"net/http"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/timeutils"
)

func (s *Scheduler) scheduleNewDagRunHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != "POST" {
		http.Error(w, "Only POST requests are allowed",
			http.StatusMethodNotAllowed)
		return
	}

	dagTriggerInput, decodeErr := decode[api.DagRunTriggerInput](r)
	if decodeErr != nil {
		http.Error(w, decodeErr.Error(), http.StatusBadRequest)
		return
	}

	if _, exists := s.dags[dag.Id(dagTriggerInput.DagId)]; !exists {
		http.Error(w, "Incorrect DAG ID", http.StatusNotFound)
		return
	}

	execTs := timeutils.Now()
	execTsStr := timeutils.ToString(execTs)
	newDr := DagRun{
		DagId:  dag.Id(dagTriggerInput.DagId),
		AtTime: execTs,
	}

	s.logger.Info("New DAG run schedule triggered externally", "dagId",
		dagTriggerInput.DagId, "execTs", timeutils.ToString(execTs))

	timeout := s.config.TaskSchedulerConfig.PutOnTaskQueueTimeout
	ctx, cancelFunc := context.WithTimeout(r.Context(), timeout)
	defer cancelFunc()

	runId, iErr := s.dbClient.InsertDagRun(
		ctx, dagTriggerInput.DagId, execTsStr,
	)
	if iErr != nil {
		s.logger.Error("Cannot insert new DAG run into database", "dagId",
			dagTriggerInput.DagId, "execTs", execTsStr, "err", iErr.Error())
	}
	uErr := s.dbClient.UpdateDagRunStatus(
		ctx, runId, dag.RunReadyToSchedule.String(),
	)
	if uErr != nil {
		s.logger.Warn("Cannot update dag run status to READY_TO_SCHEDULE",
			"dagId", dagTriggerInput.DagId, "execTs", execTsStr, "err", uErr)
	}

	// Put new DAG run onto the queue
	ds.PutContext(ctx, s.queues.DagRuns, newDr)

	uErr = s.dbClient.UpdateDagRunStatus(ctx, runId, dag.RunScheduled.String())
	if uErr != nil {
		s.logger.Warn("Cannot update dag run status to SCHEDULED", "dagId",
			dagTriggerInput.DagId, "execTs", execTsStr, "err", uErr)
	}

	s.logger.Info("New DAG run triggered externally is scheduled", "dagId",
		dagTriggerInput.DagId, "execTs", execTs, "duration", time.Since(start))
}
