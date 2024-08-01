package scheduler

import (
	"net/http"

	"github.com/ppacer/core/api"
)

// Register HTTP server endpoints for the Scheduler.
func (s *Scheduler) registerEndpoints(mux *http.ServeMux, ts *TaskScheduler) {
	r := api.Routes()

	// /dag/task
	mux.HandleFunc(r[api.EndpointDagTaskPop].RoutePattern, ts.popTask)
	mux.HandleFunc(r[api.EndpointDagTaskUpdate].RoutePattern, ts.upsertTaskStatus)

	// /state
	mux.HandleFunc(r[api.EndpointState].RoutePattern, s.currentState)
}
