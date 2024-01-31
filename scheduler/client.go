// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/ds"
	"github.com/ppacer/core/models"
)

const (
	getTaskEndpoint          = "/dag/task/pop"
	getStateEndpoint         = "/state"
	upsertTaskStatusEndpoint = "/dag/task/update"
)

// Client provides API for interacting with Scheduler.
type Client struct {
	httpClient   *http.Client
	schedulerUrl string
}

// NewClient instantiate new Client.
func NewClient(url string, httpClient *http.Client, config ClientConfig) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: config.HttpClientTimeout}
	}
	return &Client{
		httpClient:   httpClient,
		schedulerUrl: url,
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

// UpsertTaskStatus either updates existing DAG run task status or inserts new
// one.
func (c *Client) UpsertTaskStatus(tte models.TaskToExec, status dag.TaskStatus) error {
	start := time.Now()
	statusStr := status.String()
	slog.Debug("Start updating task status", "taskToExec", tte, "status",
		statusStr)
	drts := models.DagRunTaskStatus{
		DagId:  tte.DagId,
		ExecTs: tte.ExecTs,
		TaskId: tte.TaskId,
		Status: statusStr,
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
			upsertTaskStatusEndpoint, postErr)
	}
	defer resp.Body.Close()
	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		return fmt.Errorf("cannot read POST %s response body: %s",
			upsertTaskStatusEndpoint, rErr.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error with status %s for POST %s: %s",
			resp.Status, upsertTaskStatusEndpoint, string(body))
	}
	slog.Debug("Updated task status", "taskToExec", tte, "status", status,
		"duration", time.Since(start))
	return nil
}

// GetState gets the current Scheduler state.
func (c *Client) GetState() (State, error) {
	resp, err := c.httpClient.Get(c.getStateUrl())
	if err != nil {
		return 0, fmt.Errorf("error while making HTTP request: %w", err)
	}
	defer resp.Body.Close()

	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		slog.Error("Could not read GetTask response body", "err", rErr)
		return 0, rErr
	}

	var stateJson struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &stateJson); err != nil {
		return 0, fmt.Errorf("error while decoding JSON response: %w", err)
	}
	return ParseState(stateJson.State)
}

// Stop stops the Scheduler.
func (c *Client) Stop() error {
	// TODO
	return nil
}

func (c *Client) getTaskUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, getTaskEndpoint)
}

func (c *Client) getStateUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, getStateEndpoint)
}

func (c *Client) getUpdateTaskStatusUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, upsertTaskStatusEndpoint)
}
