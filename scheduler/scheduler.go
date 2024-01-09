package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dskrzypiec/scheduler/dag"
	"github.com/dskrzypiec/scheduler/db"
	"github.com/dskrzypiec/scheduler/ds"
	"github.com/dskrzypiec/scheduler/models"
	"github.com/dskrzypiec/scheduler/timeutils"
)

// Scheduler is the title object of this package, it connects all other
// components together. There should be single instance of a scheduler in
// single project - currently model of 1 scheduler and N executors are
// supported.
type Scheduler struct {
	dbClient *db.Client
	config   Config
	queues   Queues
}

// New returns new instance of Scheduler. Scheduler needs database client,
// queues for asynchronous communication and configuration. Database clients
// are by default available in db package (e.g. db.NewSqliteClient). Default
// configuration is set in DefaultConfig and default fixed-size buffer queues
// in DefaultQueues.
func New(dbClient *db.Client, queues Queues, config Config) *Scheduler {
	return &Scheduler{
		dbClient: dbClient,
		config:   config,
		queues:   queues,
	}
}

// Start starts Scheduler. It synchronize internal queues with the database,
// fires up DAG watcher, task scheduler and finally returns HTTP ServeMux
// with attached HTTP endpoints for communication between scheduler and
// executors. TODO(dskrzypiec): more docs
func (s *Scheduler) Start() http.Handler {
	cacheSize := s.config.DagRunTaskCacheLen
	taskCache := ds.NewLruCache[DagRunTask, DagRunTaskState](cacheSize)

	// Syncing queues with the database in case of program restarts.
	syncWithDatabase(s.queues.DagRuns, s.dbClient, s.config)
	//syncDagRunTaskCache(context.TODO(), taskCache, s.dbClient) // TODO

	dagRunWatcher := NewDagRunWatcher(
		s.queues.DagRuns, s.dbClient, s.config.DagRunWatcherConfig,
	)

	taskScheduler := TaskScheduler{
		DbClient:    s.dbClient,
		DagRunQueue: s.queues.DagRuns,
		TaskQueue:   s.queues.DagRunTasks,
		TaskCache:   taskCache,
		Config:      s.config.TaskSchedulerConfig,
	}

	go func() {
		// Running in the background dag run watcher
		// TODO(dskrzypiec): Probably move it as Start parameter (dags
		// []dag.Dag).
		dagRunWatcher.Watch(dag.List())
	}()

	go func() {
		// Running in the background task scheduler
		taskScheduler.Start()
	}()

	mux := http.NewServeMux()
	s.registerEndpoints(mux, &taskScheduler)

	return mux
}

func (s *Scheduler) registerEndpoints(mux *http.ServeMux, ts *TaskScheduler) {
	mux.HandleFunc("/dag/task/pop", ts.popTask)
	mux.HandleFunc("/dag/task/update", ts.updateTaskStatus)
}

// HTTP handler for popping dag run task from the queue.
func (ts *TaskScheduler) popTask(w http.ResponseWriter, _ *http.Request) {
	if ts.TaskQueue.Size() == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	drt, err := ts.TaskQueue.Pop()
	if err == ds.ErrQueueIsEmpty {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		errMsg := fmt.Sprintf("cannot get scheduled task from the queue: %s",
			err.Error())
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	drtmodel := models.TaskToExec{
		DagId:  string(drt.DagId),
		ExecTs: timeutils.ToString(drt.AtTime),
		TaskId: drt.TaskId,
	}
	jsonBytes, jsonErr := json.Marshal(drtmodel)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}

// Updates task status in the task cache and the database.
func (ts *TaskScheduler) updateTaskStatus(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != "POST" {
		http.Error(w, "Only POST requests are allowed",
			http.StatusMethodNotAllowed)
		return
	}

	var drts models.DagRunTaskStatus
	err := json.NewDecoder(r.Body).Decode(&drts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	execTs, tErr := timeutils.FromString(drts.ExecTs)
	if tErr != nil {
		msg := fmt.Sprintf("Given execTs timestamp in incorrect format: %s",
			tErr.Error())
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	status, statusErr := dag.ParseTaskStatus(drts.Status)
	if statusErr != nil {
		msg := fmt.Sprintf("Incorrect dag run task status: %s",
			statusErr.Error())
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	drt := DagRunTask{
		DagId:  dag.Id(drts.DagId),
		AtTime: execTs,
		TaskId: drts.TaskId,
	}

	ctx := context.TODO()
	updateErr := ts.UpsertTaskStatus(ctx, drt, status)
	if updateErr != nil {
		msg := fmt.Sprintf("Error while updating dag run task status: %s",
			updateErr.Error())
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	slog.Debug("Updated task status", "dagruntask", drt, "status", status,
		"duration", time.Since(start))
}
