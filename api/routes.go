// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package api provieds information about Scheduler HTTP endpoints.
package api

// Identifier for Scheduler server endpoints.
type EndpointID int

const (
	// Endpoint for taking the first task to be executed from Scheduler
	// internal queue.
	EndpointDagTaskPop EndpointID = iota

	// Endpoint for updating status of given DAG run task.
	EndpointDagTaskUpdate

	// Endpoint return current status of Scheduler.
	EndpointState

	// Endpoint returns statistics on DAG runs and scheduler queues.
	EndpointUiDagrunStats

	// Endpoint returns data on N latest DAG runs.
	EndpointUiDagrunLatest
)

// Endpoint contains information about an HTTP endpoint.
type Endpoint struct {
	RoutePattern string
	UrlSuffix    string
}

// Routes for all Scheduler server endpoints.
func Routes() map[EndpointID]Endpoint {
	return map[EndpointID]Endpoint{
		// /dag/task/*
		EndpointDagTaskPop:    {"GET /dag/task/pop", "/dag/task/pop"},
		EndpointDagTaskUpdate: {"POST /dag/task/update", "/dag/task/update"},

		// /state
		EndpointState: {"GET /state", "/state"},

		// /ui/dagrun/*
		EndpointUiDagrunStats:  {"GET /ui/dagrun/stats", "/ui/dagrun/stats"},
		EndpointUiDagrunLatest: {"GET /ui/dagrun/latest/{n}", "/ui/dagrun/latest"},
	}
}
