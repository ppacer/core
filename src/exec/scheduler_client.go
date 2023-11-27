package exec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/dskrzypiec/scheduler/src/ds"
	"github.com/dskrzypiec/scheduler/src/models"
)

const (
	getTaskEndpoint          = "/dag/task/pop"
	updateTaskStatusEndpoint = "/dag/task/update"
)

type SchedulerClient struct {
	httpClient   *http.Client
	schedulerUrl string
}

// Instantiate new Client.
func NewSchedulerClient(
	schedulerUrl string, httpClient *http.Client,
) *SchedulerClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &SchedulerClient{
		httpClient:   httpClient,
		schedulerUrl: schedulerUrl,
	}
}

// GetTask gets new task from scheduler to be executed by executor.
func (c *SchedulerClient) GetTask() (models.TaskToExec, error) {
	startTs := time.Now()
	var taskToExec models.TaskToExec

	resp, err := c.httpClient.Get(c.getTaskUrl())
	if err != nil {
		slog.Error("GetTask failed", "err", err)
		return taskToExec, err
	}

	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		slog.Error("Could not read GetTask response body", "err", rErr)
		return taskToExec, rErr
	}

	if resp.StatusCode == http.StatusNoContent {
		return taskToExec, ds.ErrQueueIsEmpty
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("Got status code != 200 on GetTask response", "statuscode",
			resp.StatusCode, "body", string(body))
		return taskToExec, fmt.Errorf("got %d status code in GetTask response",
			resp.StatusCode)
	}

	jErr := json.Unmarshal(body, &taskToExec)
	if jErr != nil {
		slog.Error("Unmarshal into APIResponse failed", "body", string(body),
			"err", jErr)
		return taskToExec, fmt.Errorf("couldn't unmarshal into telegram models.TaskToExec: %s",
			jErr.Error())
	}
	slog.Debug("GetTask finished", "duration", time.Since(startTs))
	return taskToExec, nil
}

func (c *SchedulerClient) UpdateTaskStatus(
	tte models.TaskToExec, status string,
) error {
	start := time.Now()
	slog.Debug("Start updating task status", "taskToExec", tte, "status", status)
	drts := models.DagRunTaskStatus{
		DagId:  tte.DagId,
		ExecTs: tte.ExecTs,
		TaskId: tte.TaskId,
		Status: status,
	}
	drtsJson, jErr := json.Marshal(drts)
	if jErr != nil {
		return fmt.Errorf("cannot marshal DagRunTaskStatus: %s", jErr.Error())
	}
	resp, postErr := c.httpClient.Post(
		c.getUpdateTaskStatusUrl(),
		"application/json",
		bytes.NewBuffer(drtsJson),
	)
	if postErr != nil {
		return fmt.Errorf("could not do POST %s request: %s",
			updateTaskStatusEndpoint, postErr)
	}
	defer resp.Body.Close()
	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		return fmt.Errorf("cannot read POST %s response body: %s",
			updateTaskStatusEndpoint, rErr.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error with status %s for POST %s: %s",
			resp.Status, updateTaskStatusEndpoint, string(body))
	}
	slog.Debug("Updated task status", "taskToExec", tte, "status", status,
		"duration", time.Since(start))
	return nil
}

func (c *SchedulerClient) getTaskUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, getTaskEndpoint)
}

func (c *SchedulerClient) getUpdateTaskStatusUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, updateTaskStatusEndpoint)
}
