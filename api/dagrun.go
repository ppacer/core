// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package api

// DagRunTaskStatus contains information about DAG run task status and
// potential execution error.
type DagRunTaskStatus struct {
	DagId   string  `json:"dagId"`
	ExecTs  string  `json:"execTs"`
	TaskId  string  `json:"taskId"`
	Retry   int     `json:"retry"`
	Status  string  `json:"status"`
	TaskErr *string `json:"taskError"`
}

// DagRunTriggerInput defines input structure for scheduling new DAG run
// execution.
type DagRunTriggerInput struct {
	DagId string `json:"dagId"`
	// TODO: Add input parameters for DAG run.
}

// DagRunRestartInput defines input structure for restarting DAG run.
type DagRunRestartInput struct {
	DagId  string `json:"dagId"`
	ExecTs string `json:"execTs"`
}
