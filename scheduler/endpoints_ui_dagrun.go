package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

const (
	// Timeout for HTTP request contexts.
	HTTPRequestContextTimeout = 30 * time.Second

	defaultDagRunsListLen = 25
)

// HTTP handler for current statistics on DAG runs, Scheduler queues and
// goroutines for the main ui page.
func (s *Scheduler) uiDagrunStatsHandler(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	s.logger.Debug("Start uiDagrunStatsHandler...")
	ctx, cancel := context.WithTimeout(
		context.Background(), HTTPRequestContextTimeout,
	)
	defer cancel()

	dagrunStats, drErr := dagrunsDbStats(
		ctx, s.dbClient.ReadDagRunsAggByStatus,
	)
	if drErr != nil {
		errMsg := fmt.Sprintf("cannot get DAG runs stats from database: %s",
			drErr.Error())
		s.logger.Error(errMsg)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	tasksStats, tErr := dagrunsDbStats(
		ctx, s.dbClient.ReadDagRunTasksAggByStatus,
	)
	if tErr != nil {
		errMsg := fmt.Sprintf("cannot get DAG runs stats from database: %s",
			tErr.Error())
		s.logger.Error(errMsg)
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
	s.logger.Debug("Handler uiDagrunStatsHandler is finished", "duration",
		time.Since(start))
}

// HTTP handler for listing latest DAG runs and information about their tasks.
func (s *Scheduler) uiDagrunListHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.logger.Debug("Start uiDagrunLatest...")
	ctx, cancel := context.WithTimeout(
		context.Background(), HTTPRequestContextTimeout,
	)
	defer cancel()

	n := parseDagRunsListLen(r, s.logger)

	dagruns, dbErr := s.dbClient.ReadDagRunsWithTaskInfo(ctx, n)
	if dbErr != nil {
		errMsg := fmt.Sprintf("cannot read list of DAG runs from database: %s",
			dbErr.Error())
		s.logger.Error(errMsg)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	s.logger.Debug("Read dagruns with task info from database", "len",
		len(dagruns))

	dagrunList := prepDagrunList(dagruns)

	encodeErr := encode(w, http.StatusOK, dagrunList)
	if encodeErr != nil {
		s.logger.Error("Cannot encode UiDagrunList", "len", len(dagruns),
			"err", encodeErr.Error())
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
	s.logger.Debug("Handler uiDagrunListHandler is finished", "duration",
		time.Since(start))
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

func parseDagRunsListLen(r *http.Request, logger *slog.Logger) int {
	n := r.PathValue("n")
	if n == "" {
		logger.Warn("Got empty value for number of DAG runs")
		return defaultDagRunsListLen
	}
	value, castErr := strconv.Atoi(n)
	if castErr != nil {
		logger.Warn("Cannot cast expected number of DAG runs <n> into int",
			"n", n)
		return defaultDagRunsListLen
	}
	return value
}

func prepDagrunList(dagruns []db.DagRunWithTaskInfo) api.UiDagrunList {
	tsTransform := func(tsStr string) api.Timestamp {
		return api.ToTimestamp(timeutils.FromStringMust(tsStr))
	}
	res := make(api.UiDagrunList, len(dagruns))
	for idx, dr := range dagruns {
		updateTs := timeutils.FromStringMust(dr.DagRun.StatusUpdateTs)
		duration := updateTs.Sub(timeutils.FromStringMust(dr.DagRun.InsertTs))
		res[idx] = api.UiDagrunRow{
			RunId:            dr.DagRun.RunId,
			DagId:            dr.DagRun.DagId,
			ExecTs:           tsTransform(dr.DagRun.ExecTs),
			InsertTs:         tsTransform(dr.DagRun.InsertTs),
			Status:           dr.DagRun.Status,
			StatusUpdateTs:   tsTransform(dr.DagRun.StatusUpdateTs),
			Duration:         duration.String(),
			TaskNum:          dr.TaskNum,
			TaskCompletedNum: dr.TaskCompletedNum,
		}
	}
	return res
}
