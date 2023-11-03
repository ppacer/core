package sched

import (
	"fmt"
	"net/http"

	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/ds"
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
	s.RegisterEndpoints(mux)

	return mux
}

func (s *Scheduler) RegisterEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/hello", helloHandler) // TODO
	// TODO: the rest of endpoints in here
}

func helloHandler(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(w, "Hello")
}
