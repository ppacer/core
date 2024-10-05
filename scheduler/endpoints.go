// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"net/http"

	"github.com/ppacer/core/api"
)

// Register HTTP server endpoints for the Scheduler.
func (s *Scheduler) registerEndpoints(mux *http.ServeMux, ts *TaskScheduler) {
	r := api.Routes()
	rp := func(e api.EndpointID) string {
		return r[e].RoutePattern
	}

	// /dag/run/*
	mux.HandleFunc(rp(api.EndpointDagRunTrigger), s.scheduleNewDagRunHandler)

	// /dag/task/*
	mux.HandleFunc(rp(api.EndpointDagTaskPop), ts.popTask)
	mux.HandleFunc(rp(api.EndpointDagTaskUpdate), ts.upsertTaskStatus)

	// /state
	mux.HandleFunc(rp(api.EndpointState), s.currentState)

	// /ui/dagrun/*
	mux.HandleFunc(rp(api.EndpointUiDagrunStats), s.uiDagrunStatsHandler)
	mux.HandleFunc(rp(api.EndpointUiDagrunLatest), s.uiDagrunListHandler)
	mux.HandleFunc(rp(api.EndpointUiDagrunDetails), s.uiDagrunDetailsHandler)
	mux.HandleFunc(rp(api.EndpointUiDagrunTaskDetails), s.uiDagrunTaskDetailsHandler)
}
