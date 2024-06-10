package dag

import "time"

// TaskConfig represents Task configuration. It contains information about
// configuration of a task execution, like a timeout for executing given task
// or how many times scheduler should retry in case of failures.
type TaskConfig struct {
	Timeout            time.Duration `json:"timeout"`
	Retries            int           `json:"retries"`
	SendAlertOnRetry   bool          `json:"sendAlertOnRetry"`
	SendAlertOnFailure bool          `json:"sendAlertOnFailure"`
}

// Default task configuration. If not specified otherwise the followig
// configuration values would be used for Task scheduling and execution.
var DefaultTaskConfig = TaskConfig{
	Timeout:            1 * time.Hour,
	Retries:            0,
	SendAlertOnRetry:   false,
	SendAlertOnFailure: true,
}

// TaskConfigFunc is a family of functions which takes a TaskConfig and
// potentially updates values of given configuration.
type TaskConfigFunc func(*TaskConfig)

// WithTaskTimeout returns TaskConfigFunc for setting a timeout for task
// exection.
func WithTaskTimeout(timeout time.Duration) TaskConfigFunc {
	return func(config *TaskConfig) {
		config.Timeout = timeout
	}
}

// WithTaskRetries returns TaskConfigFunc for setting number of retries for
// task execution.
func WithTaskRetries(retries int) TaskConfigFunc {
	return func(config *TaskConfig) {
		config.Retries = retries
	}
}

// WithTaskSendAlertOnRetries is a TaskConfigFunc which sets sending alerts on
// task retries.
func WithTaskSendAlertOnRetries(config *TaskConfig) {
	config.SendAlertOnRetry = true
}

// WithTaskNotSendAlertsOnFailures is a TaskConfigFunc which sets off sending
// alerts on task failure.
func WithTaskNotSendAlertsOnFailures(config *TaskConfig) {
	config.SendAlertOnFailure = false
}
