// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package api

// UiDagrunStats is a struct for statistics on DAG runs and related metrics for
// the main UI page.
type UiDagrunStats struct {
	DagrunsRunning        int `json:"dagrunsRunning"`
	DagrunsFailed         int `json:"dagrunsFailed"`
	DagrunQueueLen        int `json:"dagrunQueueLen"`
	TaskSchedulerQueueLen int `json:"taskSchedulerQueueLen"`
	TaskRunningNum        int `json:"taskRunningNum"`
	GoroutinesNum         int `json:"goroutinesNum"`
}
