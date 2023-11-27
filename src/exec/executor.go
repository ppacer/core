package exec

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/models"
)

// TODO(dskrzypiec): Unify those statuses, sched tasks statuses, DB, etc...
// Where? Not sure yet, perhaps src/models?
const (
	TaskStatusRunning = "RUNNING"
	TaskStatusFailed  = "FAILED"
	TaskStatusSuccess = "SUCCESS"
)

type Executor struct {
	schedClient *SchedulerClient
}

func New(schedAddr string) *Executor {
	httpClient := &http.Client{Timeout: 30 * time.Second} // TODO: config
	sc := NewSchedulerClient(schedAddr, httpClient)
	return &Executor{schedClient: sc}
}

// Start starts executor. TODO...
func (e *Executor) Start() {
	for {
		tte, err := e.schedClient.GetTask()
		if err == ds.ErrQueueIsEmpty {
			time.Sleep(10 * time.Millisecond) // TODO: config
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
		go executeTask(tte, task, e.schedClient)
	}
}

func executeTask(
	tte models.TaskToExec, task dag.Task, schedClient *SchedulerClient,
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
