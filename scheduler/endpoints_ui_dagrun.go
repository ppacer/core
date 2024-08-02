package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
)

// Timeout for HTTP request contexts.
const HTTPRequestContextTimeout = 30 * time.Second

// HTTP handler for current statistics on DAG runs, Scheduler queues and
// goroutines for the main ui page.
func (s *Scheduler) uiDagrunStatsHandler(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), HTTPRequestContextTimeout)
	defer cancel()

	dagrunStats, drErr := dagrunsDbStats(
		ctx, s.dbClient.ReadDagRunsAggByStatus,
	)
	if drErr != nil {
		errMsg := fmt.Sprintf("cannot get DAG runs stats from database: %s",
			drErr.Error())
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	tasksStats, tErr := dagrunsDbStats(
		ctx, s.dbClient.ReadDagRunTasksAggByStatus,
	)
	if tErr != nil {
		errMsg := fmt.Sprintf("cannot get DAG runs stats from database: %s",
			tErr.Error())
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	stats := api.UiDagrunStats{
		Dagruns:               dagrunStats,
		DagrunTasks:           tasksStats,
		DagrunQueueLen:        s.queues.DagRuns.Size(),
		TaskSchedulerQueueLen: s.queues.DagRunTasks.Size(),
		GoroutinesNum:         s.Goroutines(),
	}

	encodeErr := encode(w, http.StatusOK, stats)
	if encodeErr != nil {
		s.logger.Error("Cannot encode UiDagrunStats", "obj", stats, "err",
			encodeErr.Error())
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
}

func dagrunsDbStats(
	ctx context.Context, statsReader func(context.Context) (map[string]int, error),
) (api.StatusCounts, error) {
	var counts api.StatusCounts
	cntByStatus, rErr := statsReader(ctx)
	if rErr != nil {
		return counts, rErr
	}
	if cntSuccess, successExists := cntByStatus[dag.RunSuccess.String()]; successExists {
		counts.Success = cntSuccess
	}
	if cntRunning, runningExists := cntByStatus[dag.RunRunning.String()]; runningExists {
		counts.Running = cntRunning
	}
	if cntFailed, failedExists := cntByStatus[dag.RunFailed.String()]; failedExists {
		counts.Failed = cntFailed
	}
	return counts, nil
}
