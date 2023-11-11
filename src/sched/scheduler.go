package sched

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/models"
	"github.com/dskrzypiec/scheduler/src/timeutils"
)

type Scheduler struct {
	dbClient *db.Client
}

func New(dbClient *db.Client) *Scheduler {
	return &Scheduler{dbClient: dbClient}
}

// Start starts scheduler. It syncs queues with the database, fires up DAG
// watcher and task scheduler and finally returns HTTP ServeMux with attached
// HTTP endpoints for communication between scheduler and executors.
// TODO(dskrzypiec): more docs
func (s *Scheduler) Start() http.Handler {
	drQueue := ds.NewSimpleQueue[DagRun](1000)
	taskQueue := ds.NewSimpleQueue[DagRunTask](1000)
	taskCache := newSimpleCache[DagRunTask, DagRunTaskState]()

	// Syncing queues with the database in case of program restarts.
	syncWithDatabase(&drQueue, s.dbClient)
	//syncDagRunTaskCache(context.TODO(), &taskCache, s.dbClient) // TODO

	taskScheduler := taskScheduler{
		DbClient:    s.dbClient,
		DagRunQueue: &drQueue,
		TaskQueue:   &taskQueue,
		TaskCache:   &taskCache,
		Config:      defaultTaskSchedulerConfig(),
	}

	go func() {
		// Running in the background dag run watcher
		WatchDagRuns(dag.List(), &drQueue, s.dbClient)
	}()

	go func() {
		// Running in the background task scheduler
		taskScheduler.Start()
	}()

	mux := http.NewServeMux()
	s.RegisterEndpoints(mux, &taskScheduler)

	return mux
}

func (s *Scheduler) RegisterEndpoints(mux *http.ServeMux, ts *taskScheduler) {
	mux.HandleFunc("/dag/task/pop", ts.popTask)
	mux.HandleFunc("/dag/task/update", ts.updateTaskStatus)
}

// HTTP handler for popping dag run task from the queue.
func (ts *taskScheduler) popTask(w http.ResponseWriter, _ *http.Request) {
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
func (ts *taskScheduler) updateTaskStatus(w http.ResponseWriter, r *http.Request) {
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
	status, statusErr := stringToDagRunTaskStatus(drts.Status)
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
