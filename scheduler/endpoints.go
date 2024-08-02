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

	// /dag/task/*
	mux.HandleFunc(rp(api.EndpointDagTaskPop), ts.popTask)
	mux.HandleFunc(rp(api.EndpointDagTaskUpdate), ts.upsertTaskStatus)

	// /state
	mux.HandleFunc(rp(api.EndpointState), s.currentState)

	// /ui/dagrun/*
	mux.HandleFunc(rp(api.EndpointUiDagrunStats), s.uiDagrunStatsHandler)
}
