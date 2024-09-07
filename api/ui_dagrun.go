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

// UIDagrunDetails represents details on a single DAG run, including its tasks
// and their log records.
type UIDagrunDetails struct {
	RunId    int64          `json:"runId"`
	DagId    string         `json:"dagId"`
	ExecTs   Timestamp      `json:"execTs"`
	Status   string         `json:"status"`
	Duration string         `json:"duration"`
	Tasks    []UIDagrunTask `json:"tasks"`
}

// UIDagrunTask represents information of single DAG run task, including
// execution logs.
type UIDagrunTask struct {
	TaskId        string     `json:"taskId"`
	Retry         int        `json:"retry"`
	InsertTs      Timestamp  `json:"insertTs"`
	TaskNoStarted bool       `json:"taskNotStarted"`
	Status        string     `json:"status"`
	Pos           TaskPos    `json:"taskPosition"`
	Duration      string     `json:"duration"`
	Config        string     `json:"configJson"`
	TaskLogs      UITaskLogs `json:"taskLogs"`
}

// TaskPos represents a Task position in a DAG. Root starts in (D=1,W=1).
type TaskPos struct {
	Depth int `json:"depth"`
	Width int `json:"width"`
}

// UITaskLogs represents information on DAG run task logs. By default read only
// fixed number of log records, to limit the request body size and on demand
// more log records can be loaded in a separate call.
type UITaskLogs struct {
	LogRecordsCount int               `json:"logRecordsCount"`
	LoadedRecords   int               `json:"loadedRecords"`
	Records         []UITaskLogRecord `json:"logRecords"`
}

// UITaskLogRecord represents task log records with assumed DAG run and task
// information.
type UITaskLogRecord struct {
	InsertTs       Timestamp `json:"insertTs"`
	Level          string    `json:"level"`
	Message        string    `json:"message"`
	AttributesJson string    `json:"attributesJson"`
}
