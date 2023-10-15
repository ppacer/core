package sched_client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/dskrzypiec/scheduler/src/models"
	"github.com/rs/zerolog/log"
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
		log.Error().Err(err).Dur("durationMs", time.Since(startTs)).Msgf("[%s] GetTask failed", prefix)
		return taskToExec, err
	}

	body, rErr := ioutil.ReadAll(resp.Body)
	if rErr != nil {
		log.Error().Err(rErr).Dur("durationMs", time.Since(startTs)).
			Msgf("[%s] couldn't read GetTask response body", prefix)
		return taskToExec, rErr
	}

	if resp.StatusCode != http.StatusOK {
		log.Error().Int("statuscode", resp.StatusCode).
			Str("respBody", string(body)).Dur("durationMs", time.Since(startTs)).
			Msgf("[%s] got status code != 200 on GetTask response", prefix)
		return taskToExec, fmt.Errorf("got %d status code in GetTask response", resp.StatusCode)
	}

	jErr := json.Unmarshal(body, &taskToExec)
	if jErr != nil {
		log.Error().Err(jErr).Str("respBody", string(body)).Dur("durationMs", time.Since(startTs)).
			Msgf("[%s] unmarshal into APIResponse failed", prefix)
		return taskToExec, fmt.Errorf("couldn't unmarshal into telegram models.TaskToExec: %s", jErr.Error())
	}

	log.Info().Dur("durationMs", time.Since(startTs)).Msg("GetTask finished")
	return taskToExec, nil
}

func (c *Client) getTaskUrl() string {
	return fmt.Sprintf("%s%s", c.schedulerUrl, getTaskEndpoint)
}
