// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ppacer/core/api"
	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/ds"
)

// Client provides API for interacting with Scheduler.
type Client struct {
	httpClient   *http.Client
	schedulerUrl string
	logger       *slog.Logger
	routes       map[api.EndpointID]api.Endpoint
}

// NewClient instantiate new Client. In case when HTTP client or logger are
// nil, those would be initialized with default parameters.
func NewClient(url string, httpClient *http.Client, logger *slog.Logger, config ClientConfig) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: config.HttpClientTimeout}
	}
	if logger == nil {
		logger = defaultLogger()
	}
	return &Client{
		httpClient:   httpClient,
		schedulerUrl: url,
		logger:       logger,
		routes:       api.Routes(),
	}
}

// GetTask gets new task from scheduler to be executed by executor.
func (c *Client) GetTask() (api.TaskToExec, error) {
	startTs := time.Now()
	var taskToExec api.TaskToExec

	resp, err := c.httpClient.Get(c.getTaskUrl())
	if err != nil {
		return taskToExec, err
	}

	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		return taskToExec, rErr
	}

	if resp.StatusCode == http.StatusNoContent {
		return taskToExec, ds.ErrQueueIsEmpty
	}

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("Got status code != 200 on GetTask response", "statuscode",
			resp.StatusCode, "body", string(body))
		return taskToExec, fmt.Errorf("got %d status code in GetTask response",
			resp.StatusCode)
	}

	jErr := json.Unmarshal(body, &taskToExec)
	if jErr != nil {
		c.logger.Error("Unmarshal into APIResponse failed", "body", string(body),
			"err", jErr)
		return taskToExec, fmt.Errorf("couldn't unmarshal into telegram api.TaskToExec: %s",
			jErr.Error())
	}
	c.logger.Debug("GetTask finished", "duration", time.Since(startTs))
	return taskToExec, nil
}

// UpsertTaskStatus either updates existing DAG run task status or inserts new
// one.
func (c *Client) UpsertTaskStatus(tte api.TaskToExec, status dag.TaskStatus, taskErr error) error {
	start := time.Now()
	statusStr := status.String()
	c.logger.Debug("Start updating task status", "taskToExec", tte, "status",
		statusStr)
	var taskErrStr *string = nil
	if taskErr != nil {
		tmp := taskErr.Error()
		taskErrStr = &tmp
	}
	drts := api.DagRunTaskStatus{
		DagId:   tte.DagId,
		ExecTs:  tte.ExecTs,
		TaskId:  tte.TaskId,
		Retry:   tte.Retry,
		Status:  statusStr,
		TaskErr: taskErrStr,
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
			c.routes[api.EndpointDagTaskUpdate].UrlSuffix, postErr)
	}
	defer resp.Body.Close()
	body, rErr := io.ReadAll(resp.Body)
	if rErr != nil {
		return fmt.Errorf("cannot read POST %s response body: %s",
			c.routes[api.EndpointDagTaskUpdate].UrlSuffix, rErr.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error with status %s for POST %s: %s",
			resp.Status, c.routes[api.EndpointDagTaskUpdate].UrlSuffix,
			string(body))
	}
	c.logger.Debug("Updated task status", "taskToExec", tte, "status", status,
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
		c.logger.Error("Could not read GetTask response body", "err", rErr)
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

func (c *Client) UiDagrunStats() (api.UiDagrunStats, error) {
	// TODO
	return api.UiDagrunStats{}, errors.New("NOT IMPLEMENTED")
}

// Stop stops the Scheduler.
func (c *Client) Stop() error {
	// TODO
	return nil
}

func (c *Client) getTaskUrl() string {
	suffix := c.routes[api.EndpointDagTaskPop].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}

func (c *Client) getStateUrl() string {
	suffix := c.routes[api.EndpointState].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}

func (c *Client) getUpdateTaskStatusUrl() string {
	suffix := c.routes[api.EndpointDagTaskUpdate].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}
