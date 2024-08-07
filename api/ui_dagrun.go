// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package api

// StatusCounts keeps mapping between statuses (of dagruns, task execution,
// etc) and their frequencies.
type StatusCounts struct {
	Success   int `json:"success"`
	Failed    int `json:"failed"`
	Scheduled int `json:"scheduled"`
	Running   int `json:"running"`
}

// UIDagrunStats is a struct for statistics on DAG runs and related metrics for
// the main UI page.
type UIDagrunStats struct {
	Dagruns               StatusCounts `json:"dagruns"`
	DagrunTasks           StatusCounts `json:"dagrunTasks"`
	DagrunQueueLen        int          `json:"dagrunQueueLen"`
	TaskSchedulerQueueLen int          `json:"taskSchedulerQueueLen"`
	GoroutinesNum         int          `json:"goroutinesNum"`
}

// UIDagrunRow represents information on a single DAG run on the main UI page.
type UIDagrunRow struct {
	RunId            int64     `json:"runId"`
	DagId            string    `json:"dagId"`
	ExecTs           Timestamp `json:"execTs"`
	InsertTs         Timestamp `json:"insertTs"`
	Status           string    `json:"status"`
	StatusUpdateTs   Timestamp `json:"statusUpdateTs"`
	Duration         string    `json:"duration"`
	TaskNum          int       `json:"taskNum"`
	TaskCompletedNum int       `json:"taskCompletedNum"`
}

// UIDagrunList is a slice of UiDagrunRow.
type UIDagrunList []UIDagrunRow
