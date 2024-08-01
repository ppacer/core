package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
	"github.com/ppacer/core/timeutils"
)

// HTTP handler for popping dag run task from the queue.
func (ts *TaskScheduler) popTask(w http.ResponseWriter, _ *http.Request) {
	if ts.taskQueue.Size() == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	drt, err := ts.taskQueue.Pop()
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
		Retry:  drt.Retry,
	}
	jsonBytes, jsonErr := json.Marshal(drtmodel)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}

// Updates task status in the task cache and the database.
func (ts *TaskScheduler) upsertTaskStatus(w http.ResponseWriter, r *http.Request) {
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
		Retry:  drts.Retry,
	}

	ctx := context.TODO()
	updateErr := ts.UpsertTaskStatus(ctx, drt, status, drts.TaskErr)
	if updateErr != nil {
		msg := fmt.Sprintf("Error while updating dag run task status: %s",
			updateErr.Error())
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	ts.logger.Debug("Updated task status", "dagruntask", drt, "status", status,
		"duration", time.Since(start))
}
