// Package models contains types used for communication between executors and the scheduler.
package models

type TaskToExec struct {
	DagId  string `json:"dagId"`
	ExecTs string `json:"execTs"`
	TaskId string `json:"taskId"`
}

type DagRunTaskStatus struct {
	DagId  string `json:"dagId"`
	ExecTs string `json:"execTs"`
	TaskId string `json:"taskId"`
	Status string `json:"status"`
}
