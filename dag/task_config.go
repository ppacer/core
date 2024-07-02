package dag

import (
	"text/template"

	"github.com/ppacer/core/notify"
)

// TaskConfig represents Task configuration. It contains information about
// configuration of a task execution, like a timeout for executing given task
// or how many times scheduler should retry in case of failures.
type TaskConfig struct {
	TimeoutSeconds     int  `json:"timeoutSeconds"`
	Retries            int  `json:"retries"`
	SendAlertOnRetry   bool `json:"sendAlertOnRetry"`
	SendAlertOnFailure bool `json:"sendAlertOnFailure"`

	// Notification sender for that task. By default is nil which mean that
	// notifier set on scheduler or executor level would be used. If we want to
	// use non-default notification sender for this task, we should set
	// Notifier field to non-nil instance. One can use WithCustomNotifier
	// method, to do so.
	Notifier notify.Sender `json:"-"`

	AlertOnRetryTemplate   notify.Template `json:"-"`
	AlertOnFailureTemplate notify.Template `json:"-"`
}

// Default template for alerts.
func DefaultAlertTemplate() *template.Template {
	body := `
Task [{{.TaskId}}] in DAG [{{.DagId}}] at {{.ExecTs}} has failed.
{{- if .TaskRunError}}
Error:
	{{.TaskRunError.Error}}
{{end}}
`
	return template.Must(template.New("default").Parse(body))
}

// Default task configuration. If not specified otherwise the following
// configuration values would be used for Task scheduling and execution.
var DefaultTaskConfig = TaskConfig{
	TimeoutSeconds:     10 * 60,
	Retries:            0,
	SendAlertOnRetry:   false,
	SendAlertOnFailure: true,

	// By default Notifier is inherited from either scheduler or executor
	// (depending on the context).
	Notifier: nil,

	AlertOnRetryTemplate:   DefaultAlertTemplate(),
	AlertOnFailureTemplate: DefaultAlertTemplate(),
}

// TaskConfigFunc is a family of functions which takes a TaskConfig and
// potentially updates values of given configuration.
type TaskConfigFunc func(*TaskConfig)

// WithTaskTimeout returns TaskConfigFunc for setting a timeout for task
// exection.
func WithTaskTimeout(timeoutSeconds int) TaskConfigFunc {
	return func(config *TaskConfig) {
		config.TimeoutSeconds = timeoutSeconds
	}
}

// WithTaskRetries returns TaskConfigFunc for setting number of retries for
// task execution.
func WithTaskRetries(retries int) TaskConfigFunc {
	return func(config *TaskConfig) {
		config.Retries = retries
	}
}

// WithCustomNotifier returns TaskConfigFunc for setting a custom notification
// sender for the task.
func WithCustomNotifier(notifier notify.Sender) TaskConfigFunc {
	return func(config *TaskConfig) {
		config.Notifier = notifier
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
