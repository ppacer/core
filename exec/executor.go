package exec

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/scheduler"
)

type Executor struct {
	schedClient *scheduler.Client
	config      Config
}

// Executor configuration.
type Config struct {
	PollInterval       time.Duration
	HttpRequestTimeout time.Duration
}

// Setup default configuration values.
func defaultConfig() Config {
	return Config{
		PollInterval:       10 * time.Millisecond,
		HttpRequestTimeout: 30 * time.Second,
	}
}

// New creates new Executor instance. When config is nil, then default
// configuration values will be used.
func New(schedAddr string, config *Config) *Executor {
	var cfg Config
	if config != nil {
		cfg = *config
	} else {
		cfg = defaultConfig()
	}
	httpClient := &http.Client{Timeout: cfg.HttpRequestTimeout}
	sc := scheduler.NewClient(schedAddr, httpClient, scheduler.DefaultClientConfig)
	return &Executor{
		schedClient: sc,
		config:      cfg,
	}
}

// Start starts executor. TODO...
func (e *Executor) Start(dags dag.Registry) {
	for {
		tte, err := e.schedClient.GetTask()
		if err == ds.ErrQueueIsEmpty {
			time.Sleep(e.config.PollInterval)
			continue
		}
		if err != nil {
			slog.Error("GetTask error", "err", err)
			break
		}
		slog.Info("Start executing task", "taskToExec", tte)
		d, dagExists := dags[dag.Id(tte.DagId)]
		if !dagExists {
			slog.Error("Could not get DAG from registry", "dagId", tte.DagId)
			continue
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
	tte models.TaskToExec, task dag.Task, schedClient *scheduler.Client,
) {
	defer func() {
		if r := recover(); r != nil {
			schedClient.UpdateTaskStatus(tte, dag.TaskFailed.String())
			slog.Error("Recovered from panic:", "err", r, "stack",
				string(debug.Stack()))
		}
	}()
	uErr := schedClient.UpdateTaskStatus(tte, dag.TaskRunning.String())
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			dag.TaskRunning.String(), "err", uErr.Error())
	}
	task.Execute()
	slog.Info("Finished executing task", "taskToExec", tte)
	uErr = schedClient.UpdateTaskStatus(tte, dag.TaskSuccess.String())
	if uErr != nil {
		slog.Error("Error while updating status", "tte", tte, "status",
			dag.TaskSuccess.String(), "err", uErr.Error())
	}
}
