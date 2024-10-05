// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package api

// TaskToExec contains information about a task which is scheduled to be
// executed. Primarily to communicate between Scheduler and Executors.
type TaskToExec struct {
	DagId  string `json:"dagId"`
	ExecTs string `json:"execTs"`
	TaskId string `json:"taskId"`
	Retry  int    `json:"retry"`
}
