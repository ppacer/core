// Package models contains types used for communication between executors and the scheduler.
package models

type TaskToExec struct {
	RunId  string `json:"runId"`
	DagId  string `json:"dagId"`
	TaskId string `json:"taskId"`
}
