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
	"syscall"
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
func NewClient(
	url string, httpClient *http.Client, logger *slog.Logger,
	config ClientConfig,
) *Client {
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
	tte, code, err := httpGetJSON[api.TaskToExec](c.httpClient, c.getTaskUrl())
	if code == http.StatusNoContent {
		return api.TaskToExec{}, ds.ErrQueueIsEmpty
	}
	if err != nil {
		if !errors.Is(err, syscall.ECONNREFUSED) {
			c.logger.Error("Error while getting TaskToExec", "err",
				err.Error())
		}
		return api.TaskToExec{}, err
	}
	if code != http.StatusOK {
		err := fmt.Errorf("unexpected status code in UIDagrunStats request: %d",
			code)
		return api.TaskToExec{}, err
	}
	c.logger.Debug("GetTask finished", "duration", time.Since(startTs))
	return *tte, nil
}

// UpsertTaskStatus either updates existing DAG run task status or inserts new
// one.
func (c *Client) UpsertTaskStatus(
	tte api.TaskToExec, status dag.TaskStatus, taskErr error,
) error {
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

// TriggerDagRun schedules new DAG run.
func (c *Client) TriggerDagRun(in api.DagRunTriggerInput) error {
	jsonInput, jErr := json.Marshal(in)
	if jErr != nil {
		return fmt.Errorf("cannot serialize DagRunTriggerInput: %s",
			jErr.Error())
	}
	req, rErr := http.NewRequest(
		"POST", c.triggerDagRunUrl(), bytes.NewBuffer(jsonInput),
	)
	if rErr != nil {
		return fmt.Errorf("cannot create POST request: %s", rErr.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	defer req.Body.Close()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cannot perform POST request: %s", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, rErr := io.ReadAll(resp.Body)
		if rErr != nil {
			msg := fmt.Sprintf(
				"unexpected status code in TriggerDagRun request: %d, "+
					"and cannot read the response body: %s",
				resp.StatusCode, rErr.Error(),
			)
			return errors.New(msg)
		}
		return fmt.Errorf("unexpected status code in TriggerDagRun request: %d. Body: %s",
			resp.StatusCode, string(body))
	}

	return nil
}

// UIDagrunStats returns the current statistics on DAG runs, its tasks and
// goroutines.
func (c *Client) UIDagrunStats() (api.UIDagrunStats, error) {
	startTs := time.Now()
	c.logger.Debug("Start UIDagrunStats request...")
	stats, code, err := httpGetJSON[api.UIDagrunStats](
		c.httpClient, c.uiDagrunStatsUrl(),
	)
	if err != nil {
		c.logger.Error("Error while getting UI DAG runs stats", "err",
			err.Error())
		return api.UIDagrunStats{}, err
	}
	if code != http.StatusOK {
		err := fmt.Errorf("unexpected status code in UIDagrunStats request: %d",
			code)
		return api.UIDagrunStats{}, err
	}
	c.logger.Debug("UIDagrunStats request finished", "duration",
		time.Since(startTs))
	return *stats, nil
}

// UIDagrunLatest returns information on latest n DAG runs and its tasks
// completion.
func (c *Client) UIDagrunLatest(n int) (api.UIDagrunList, error) {
	startTs := time.Now()
	c.logger.Debug("Start UIDagrunStats request...")
	dagruns, code, err := httpGetJSON[api.UIDagrunList](
		c.httpClient, c.uiDagrunLatestUrl(n),
	)
	if err != nil {
		c.logger.Error("Error while getting latest DAG runs", "err",
			err.Error())
		return api.UIDagrunList{}, err
	}
	if code != http.StatusOK {
		err := fmt.Errorf("unexpected status code in UIDagrunList request: %d",
			code)
		return api.UIDagrunList{}, err
	}
	c.logger.Debug("UIDagrunList request finished", "duration",
		time.Since(startTs))
	return *dagruns, nil
}

// UIDagrunDetails provides detailed information on given DAG run, including
// task logs and task configuration.
func (c *Client) UIDagrunDetails(runId int) (api.UIDagrunDetails, error) {
	startTs := time.Now()
	c.logger.Debug("Start UIDagrunDetails request...")
	dagruns, code, err := httpGetJSON[api.UIDagrunDetails](
		c.httpClient, c.uiDagrunDetailsUrl(runId),
	)
	if err != nil {
		c.logger.Error("Error while getting DAG run details", "runId", runId,
			"err", err.Error())
		return api.UIDagrunDetails{}, err
	}
	if code != http.StatusOK {
		err := fmt.Errorf("unexpected status code in UIDagRunDetails request: %d",
			code)
		return api.UIDagrunDetails{}, err
	}
	c.logger.Debug("UIDagRunDetails request finished", "duration",
		time.Since(startTs))
	return *dagruns, nil
}

// UIDagrunTaskLogs returns all task logs for given DAG run and given task ID.
func (c *Client) UIDagrunTaskDetails(runId int, taskId string, retry int) (api.UIDagrunTask, error) {
	startTs := time.Now()
	c.logger.Debug("Start UIDagrunTaskDetails request...")
	drtd, code, err := httpGetJSON[api.UIDagrunTask](
		c.httpClient, c.uiDagrunTaskDetailsUrl(runId, taskId, retry),
	)
	if err != nil {
		c.logger.Error("Error while getting DAG run task details", "runId",
			runId, "taskId", taskId, "retry", retry, "err", err.Error())
		return api.UIDagrunTask{}, err
	}
	if code != http.StatusOK {
		err := fmt.Errorf("unexpected status code in UIDagrunTaskDetails request: %d",
			code)
		return api.UIDagrunTask{}, err
	}
	c.logger.Debug("UIDagrunTaskDetails request finished", "duration",
		time.Since(startTs))
	return *drtd, nil
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

func (c *Client) triggerDagRunUrl() string {
	suffix := c.routes[api.EndpointDagRunTrigger].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}

func (c *Client) getUpdateTaskStatusUrl() string {
	suffix := c.routes[api.EndpointDagTaskUpdate].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}

func (c *Client) uiDagrunStatsUrl() string {
	suffix := c.routes[api.EndpointUiDagrunStats].UrlSuffix
	return fmt.Sprintf("%s%s", c.schedulerUrl, suffix)
}

func (c *Client) uiDagrunLatestUrl(n int) string {
	suffix := c.routes[api.EndpointUiDagrunLatest].UrlSuffix
	return fmt.Sprintf("%s%s/%d", c.schedulerUrl, suffix, n)
}

func (c *Client) uiDagrunDetailsUrl(runId int) string {
	suffix := c.routes[api.EndpointUiDagrunDetails].UrlSuffix
	return fmt.Sprintf("%s%s/%d", c.schedulerUrl, suffix, runId)
}

func (c *Client) uiDagrunTaskDetailsUrl(runId int, taskId string, retry int) string {
	suffix := c.routes[api.EndpointUiDagrunTaskDetails].UrlSuffix
	return fmt.Sprintf("%s%s/%d/%s/%d", c.schedulerUrl, suffix, runId,
		taskId, retry)
}

// Generic HTTP GET request with resp body deserialization from JSON into given
// type.
func httpGetJSON[T any](client *http.Client, url string) (*T, int, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, http.StatusBadRequest,
			fmt.Errorf("failed to perform GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode,
			fmt.Errorf("received non-200 response: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode,
			fmt.Errorf("failed to read response body: %w", err)
	}

	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, resp.StatusCode,
			fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &result, resp.StatusCode, nil
}
