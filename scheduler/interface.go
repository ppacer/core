package scheduler

import (
	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
)

// API defines ppacer Scheduler API. Client implements this interface.
type API interface {
	GetTask() (api.TaskToExec, error)
	UpsertTaskStatus(api.TaskToExec, dag.TaskStatus, error) error
	GetState() (State, error)
	UIDagrunStats() (api.UIDagrunStats, error)
	UIDagrunLatest(int) (api.UIDagrunList, error)
	UIDagrunDetails(int) (api.UIDagrunDetails, error)
	UIDagrunTaskLogs(int, string, int) (api.UITaskLogs, error)
}
