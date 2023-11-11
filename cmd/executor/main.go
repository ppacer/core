package main

import (
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/dskrzypiec/scheduler/cmd/executor/sched_client"
	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/models"

	_ "github.com/dskrzypiec/scheduler/src/user"
)

const (
	TaskStatusRunning = "RUNNING"
	TaskStatusFailed  = "FAILED"
	TaskStatusSuccess = "SUCCESS"
)

func main() {
	cfg := ParseConfig()
	cfg.setupLogger()

	schedClient := sched_client.New("http://localhost:8080", nil)

	for {
		tte, err := schedClient.GetTask()
		if err == ds.ErrQueueIsEmpty {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			slog.Error("GetTask error", "err", err)
			break
		}
		slog.Info("Start executing task", "taskToExec", tte)
		d, dErr := dag.Get(dag.Id(tte.DagId))
		if dErr != nil {
			slog.Error("Could not get DAG from registry", "dagId", tte.DagId)
			break
		}
		task, tErr := d.GetTask(tte.TaskId)
		if tErr != nil {
			slog.Error("Could not get task from DAG", "dagId", tte.DagId,
				"taskId", tte.TaskId)
			break
		}
		go executeTask(tte, task, schedClient)
	}
}

func executeTask(
	tte models.TaskToExec, task dag.Task, schedClient *sched_client.Client,
) {
	defer func() {
		if r := recover(); r != nil {
			schedClient.UpdateTaskStatus(tte, TaskStatusFailed)
			slog.Error("Recovered from panic:", "err", r, "stack",
				string(debug.Stack()))
		}
	}()
	uErr := schedClient.UpdateTaskStatus(tte, TaskStatusRunning)
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			TaskStatusRunning, "err", uErr.Error())
	}
	task.Execute()
	slog.Info("Finished executing task", "taskToExec", tte)
	uErr = schedClient.UpdateTaskStatus(tte, TaskStatusSuccess)
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			TaskStatusSuccess, "err", uErr.Error())
	}
}
