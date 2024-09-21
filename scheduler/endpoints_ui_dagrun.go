package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/dag/tasklog"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

const (
	// Timeout for HTTP request contexts.
	HTTPRequestContextTimeout = 30 * time.Second

	defaultDagRunsListLen = 25
)

var (
	ErrDagRunIdIncorrect = errors.New("incorrect DAG run ID")
)

// HTTP handler for current statistics on DAG runs, Scheduler queues and
// goroutines for the main ui page.
func (s *Scheduler) uiDagrunStatsHandler(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	s.logger.Info("Start uiDagrunStatsHandler...")
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

	stats := api.UIDagrunStats{
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
	s.logger.Info("Handler uiDagrunStatsHandler is finished", "duration",
		time.Since(start))
}

// HTTP handler for listing latest DAG runs and information about their tasks.
func (s *Scheduler) uiDagrunListHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.logger.Info("Start uiDagrunLatest...")
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
	s.logger.Info("Handler uiDagrunListHandler is finished", "duration",
		time.Since(start))
}

// HTTP handler for serving all details on given DAG run, including information
// about tasks and their log records.
func (s *Scheduler) uiDagrunDetailsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.logger.Debug("Start uiDagrunDetails...")
	ctx, cancel := context.WithTimeout(
		context.Background(), HTTPRequestContextTimeout,
	)
	defer cancel()

	runId, parseErr := getPathValueInt(r, "runId")
	if parseErr != nil {
		http.Error(w, parseErr.Error(), http.StatusBadRequest)
	}

	dagRun, drDbErr := s.dbClient.ReadDagRun(ctx, runId)
	if drDbErr == sql.ErrNoRows {
		http.Error(
			w, "there is no DAG run for given runId", http.StatusBadRequest,
		)
		return
	}
	dagrunDetails, err := s.prepDagrunDetails(dagRun)
	if err != nil {
		http.Error(w, "cannot prepare DAG run details",
			http.StatusInternalServerError)
		return
	}

	encodeErr := encode(w, http.StatusOK, dagrunDetails)
	if encodeErr != nil {
		s.logger.Error("Cannot encode UIDagRunDetails", "runId", runId,
			"err", encodeErr.Error())
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
	s.logger.Debug("Handler uiDagrunListHandler is finished", "duration",
		time.Since(start))
}

// HTTP handler for getting details for given single DAG run task.
func (s *Scheduler) uiDagrunTaskDetailsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.logger.Info("Start uiDagrunTask...")
	ctx, cancel := context.WithTimeout(
		context.Background(), HTTPRequestContextTimeout,
	)
	defer cancel()

	runId, taskId, retry, parseErr := parseDagRunTaskPathArgs(r)
	if parseErr != nil {
		s.logger.Error("Error while parsing uiDagrunTask handler args",
			"err", parseErr.Error())
		http.Error(w, "invalid path arguments", http.StatusBadRequest)
		return
	}

	dagRun, dbErr := s.dbClient.ReadDagRun(ctx, runId)
	if dbErr != nil {
		s.logger.Error("Cannot read DAG run", "runId", runId, "err",
			dbErr.Error())
		http.Error(
			w, "cannot read DAG run info from database",
			http.StatusInternalServerError,
		)
		return
	}

	dagRunTasks, dbErr := s.dbClient.ReadDagRunSingleTaskDetails(
		ctx, dagRun.DagId, dagRun.ExecTs, taskId,
	)
	if dbErr != nil {
		s.logger.Error("Cannot read DAG run task details from DB", "runId",
			runId, "dagId", dagRun.DagId, "execTs", dagRun.ExecTs, "taskId",
			taskId, "dbErr", dbErr.Error())
		http.Error(
			w, "cannot read DAG run task details from database",
			http.StatusInternalServerError,
		)
		return
	}

	task, prepErr := s.prepDagrunTaskDetails(dagRunTasks)
	if prepErr != nil {
		s.logger.Error("Cannot prepare DAG run task details", "runId",
			runId, "dagId", dagRun.DagId, "execTs", dagRun.ExecTs, "taskId",
			taskId, "prepErr", prepErr.Error())
		http.Error(
			w, "cannot prepare DAG run task details",
			http.StatusInternalServerError,
		)
		return
	}

	encodeErr := encode(w, http.StatusOK, task)
	if encodeErr != nil {
		s.logger.Error("Cannot encode UIDagrunTask", "err", encodeErr.Error())
		http.Error(
			w, "cannot serialize UIDagrunTask", http.StatusInternalServerError,
		)
		return
	}

	s.logger.Info("Handler uiDagrunTask is finished", "dagId",
		dagRun.DagId, "taskId", taskId, "retry", retry, "duration",
		time.Since(start),
	)
}

func dagrunsDbStats(
	ctx context.Context, statsReader func(context.Context) (map[string]int, error),
) (api.StatusCounts, error) {
	var counts api.StatusCounts
	cntByStatus, rErr := statsReader(ctx)
	if rErr != nil {
		return counts, rErr
	}
	cntSuccess, successExists := cntByStatus[dag.RunSuccess.String()]
	if successExists {
		counts.Success = cntSuccess
	}
	cntRunning, runningExists := cntByStatus[dag.RunRunning.String()]
	if runningExists {
		counts.Running = cntRunning
	}
	cntFailed, failedExists := cntByStatus[dag.RunFailed.String()]
	if failedExists {
		counts.Failed = cntFailed
	}
	cntScheduled, scheduledExists := cntByStatus[dag.RunScheduled.String()]
	if scheduledExists {
		counts.Scheduled = cntScheduled
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

func parseDagRunId(r *http.Request) (int, error) {
	runId := r.PathValue("runId")
	if runId == "" {
		return -1, fmt.Errorf("parameter runId is unexpectedly empty")
	}
	value, castErr := strconv.Atoi(runId)
	if castErr != nil {
		return -1, fmt.Errorf(
			"parameter runId (%s) cannot be cast as integer: %s",
			runId, castErr.Error())
	}
	return value, nil
}

func parseDagRunTaskPathArgs(r *http.Request) (int, string, int, error) {
	runId, parseRunIdErr := getPathValueInt(r, "runId")
	if parseRunIdErr != nil {
		err := fmt.Errorf("invalid runId: %w", parseRunIdErr)
		return -1, "", -1, err
	}
	taskId, parseTaskIdErr := getPathValueStr(r, "taskId")
	if parseTaskIdErr != nil {
		err := fmt.Errorf("invalid taskId: %w", parseTaskIdErr)
		return -1, "", -1, err
	}
	retry, parseRetryErr := getPathValueInt(r, "retry")
	if parseRetryErr != nil {
		err := fmt.Errorf("invalid retry argument: %w", parseRetryErr)
		return -1, "", -1, err
	}
	return runId, taskId, retry, nil
}

func prepDagrunList(dagruns []db.DagRunWithTaskInfo) api.UIDagrunList {
	tsTransform := func(tsStr string) api.Timestamp {
		return api.ToTimestamp(timeutils.FromStringMust(tsStr))
	}
	res := make(api.UIDagrunList, len(dagruns))
	for idx, dr := range dagruns {
		updateTs := timeutils.FromStringMust(dr.DagRun.StatusUpdateTs)
		duration := updateTs.Sub(timeutils.FromStringMust(dr.DagRun.InsertTs))
		res[idx] = api.UIDagrunRow{
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

func (s *Scheduler) prepDagrunDetails(dr db.DagRun) (api.UIDagrunDetails, error) {
	ctx := context.TODO()
	details := prepDagrunDetailsBase(dr)

	dagRunTasks, dbErr := s.dbClient.ReadDagRunTaskDetails(
		ctx, dr.DagId, dr.ExecTs,
	)
	if dbErr != nil {
		return details, fmt.Errorf("cannot read DAG run task details from DB: %w",
			dbErr)
	}
	tasks, tErr := s.prepDagrunDetailsTasks(dagRunTasks)
	if tErr != nil {
		return details, fmt.Errorf("cannot properly prepare DAG run task details: %s",
			tErr)
	}
	details.Tasks = tasks
	return details, nil
}

func (s *Scheduler) prepDagrunDetailsTasks(
	dagruntasks []db.DagRunTaskDetails,
) ([]api.UIDagrunTask, error) {
	result := make([]api.UIDagrunTask, 0, len(dagruntasks))

	for _, drtd := range dagruntasks {
		uiDrt, err := s.prepDagrunTaskDetails(drtd)
		if err != nil {
			s.logger.Error("Cannot prepare DAG run task details", "dagRunTask",
				drtd, "err", err.Error())
		}
		result = append(result, uiDrt)
	}
	return result, nil
}

func (s *Scheduler) prepDagrunTaskDetails(drtd db.DagRunTaskDetails) (api.UIDagrunTask, error) {
	ti := tasklog.TaskInfo{
		DagId:  drtd.DagId,
		ExecTs: timeutils.FromStringMust(drtd.ExecTs),
		TaskId: drtd.TaskId,
		Retry:  drtd.Retry,
	}
	logsReader := s.taskLogs.GetLogReader(ti)
	// TODO: We want to read just a few log records at first and load more log
	// records on demand via UI.
	logs, logsErr := logsReader.ReadAll(context.TODO())
	if logsErr != nil {
		return api.UIDagrunTask{}, logsErr
	}

	uiDrt := api.UIDagrunTask{
		TaskId:        drtd.TaskId,
		Retry:         drtd.Retry,
		InsertTs:      api.ToTimestamp(timeutils.FromStringMust(drtd.InsertTs)),
		TaskNoStarted: drtd.TaskNotStarted,
		Status:        drtd.Status,
		Pos: api.TaskPos{
			Depth: drtd.PosDepth,
			Width: drtd.PosWidth,
		},
		Duration: timeutils.Duration(drtd.InsertTs, drtd.StatusUpdateTs).String(),
		Config:   drtd.ConfigJson,
		TaskLogs: api.UITaskLogs{
			LogRecordsCount: len(logs),
			LoadedRecords:   len(logs),
			Records:         s.toUITaskLogRecords(logs),
		},
	}
	return uiDrt, nil
}

func prepDagrunDetailsBase(dr db.DagRun) api.UIDagrunDetails {
	var details api.UIDagrunDetails

	details.RunId = dr.RunId
	details.DagId = dr.DagId
	details.ExecTs = api.ToTimestamp(timeutils.FromStringMust(dr.ExecTs))
	details.Status = dr.Status
	details.Duration = timeutils.Duration(dr.InsertTs, dr.StatusUpdateTs).String()

	return details
}

func (s *Scheduler) toUITaskLogRecords(logs []tasklog.Record) []api.UITaskLogRecord {
	result := make([]api.UITaskLogRecord, 0, len(logs))
	for _, l := range logs {
		attrJson, jErr := json.Marshal(l.Attributes)
		if jErr != nil {
			s.logger.Error("toUITaskLogRecords - cannot serialize JSON logs attr",
				"err", jErr.Error())
			attrJson = []byte{}
		}
		tlr := api.UITaskLogRecord{
			InsertTs:       api.ToTimestamp(l.InsertTs),
			Level:          l.Level,
			Message:        l.Message,
			AttributesJson: string(attrJson),
		}
		result = append(result, tlr)
	}
	return result
}
