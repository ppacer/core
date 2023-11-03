package sched_client

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/dskrzypiec/scheduler/src/models"
)

const prefix = "sched_client"
const getTaskEndpoint = "/task/next"

type Client struct {
	httpClient   *http.Client
	schedulerUrl string
}

// Instantiate new Client.
func New(schedulerUrl string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		httpClient:   httpClient,
		schedulerUrl: schedulerUrl,
	}
}

// GetTask gets new task from scheduler to be executed by executor.
func (c *Client) GetTask() (models.TaskToExec, error) {
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

func (c *Client) getTaskUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, getTaskEndpoint)
}
