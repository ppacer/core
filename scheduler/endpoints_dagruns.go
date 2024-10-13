// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/schedule"
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

	timeout := s.config.TaskSchedulerConfig.PutOnTaskQueueTimeout
	ctx, cancelFunc := context.WithTimeout(r.Context(), timeout)
	defer cancelFunc()

	iSched := s.dbClient.InsertDagSchedule(
		ctx, dagTriggerInput.DagId, schedule.ManuallyTriggered.String(),
		execTsStr, &execTsStr,
	)
	if iSched != nil {
		s.logger.Error("Cannot insert new DAG schedule into database", "dagId",
			dagTriggerInput.DagId, "execTs", execTsStr, "err", iSched.Error())
		http.Error(w, "Cannot insert new DAG schedule into database",
			http.StatusInternalServerError)
		return
	}

	s.logger.Info("New DAG run schedule triggered externally", "dagId",
		dagTriggerInput.DagId, "execTs", timeutils.ToString(execTs))

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

func (s *Scheduler) restartDagRunHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != "POST" {
		http.Error(w, "Only POST requests are allowed",
			http.StatusMethodNotAllowed)
		return
	}

	input, decodeErr := decode[api.DagRunRestartInput](r)
	if decodeErr != nil {
		http.Error(w, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	execTs, parseErr := timeutils.FromString(input.ExecTs)
	if parseErr != nil {
		http.Error(w, "Incorrect DAG execTs format", http.StatusBadRequest)
	}

	if _, exists := s.dags[dag.Id(input.DagId)]; !exists {
		http.Error(w, "Incorrect DAG ID", http.StatusNotFound)
		return
	}
	timeout := s.config.TaskSchedulerConfig.PutOnTaskQueueTimeout
	ctx, cancelFunc := context.WithTimeout(r.Context(), timeout)
	defer cancelFunc()

	// Check latest DAG run status. We should only restart DAG runs that
	// FAILED.
	drDb, dbErr := s.dbClient.ReadDagRunByExecTs(
		ctx, input.DagId, input.ExecTs,
	)
	if dbErr == sql.ErrNoRows {
		http.Error(w, "DAG run not found", http.StatusNotFound)
		return
	}
	if dbErr != nil {
		s.logger.Error("Cannot read DAG run from database", "dagId",
			input.DagId, "execTs", input.ExecTs, "err", dbErr)
		http.Error(w, "Cannot read DAG run from database",
			http.StatusInternalServerError)
		return
	}
	if drDb.Status != dag.RunFailed.String() {
		http.Error(
			w, "DAG run is not in FAILED state. There will be no restart.",
			http.StatusBadRequest,
		)
		return
	}

	s.logger.Info("DAG run restart triggered", "dagId",
		input.DagId, "execTs", timeutils.ToString(execTs))

	dr := DagRun{
		DagId:       dag.Id(input.DagId),
		AtTime:      execTs,
		IsRestarted: true,
	}

	iErr := s.dbClient.InsertDagRunNextRestart(ctx, input.DagId, input.ExecTs)
	if iErr != nil {
		s.logger.Error("Cannot insert next restart for the DAG run",
			"dagId", input.DagId, "execTs", input.ExecTs, "err", iErr)
		http.Error(w, "Cannot insert next restart info into database",
			http.StatusInternalServerError)
		return
	}

	// Put new DAG run onto the queue
	ds.PutContext(ctx, s.queues.DagRuns, dr)

	uErr := s.dbClient.UpdateDagRunStatusByExecTs(ctx,
		input.DagId, input.ExecTs, dag.RunScheduled.String())
	if uErr != nil {
		s.logger.Warn("Cannot update dag run status to SCHEDULED", "dagId",
			input.DagId, "execTs", input.ExecTs, "err", uErr)
	}

	s.logger.Info("New DAG run restart is scheduled", "dagId",
		input.DagId, "execTs", input.ExecTs, "duration", time.Since(start))
}
