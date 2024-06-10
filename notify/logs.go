package notify

import (
	"context"
	"fmt"
	"log/slog"
)

// LogsErr is a notification client which "sends" notification as logs of severity
// ERROR. It implements Sender interface. It might be handy to use for local
// development or in situations when other communications channels are not yet
// in place.
type LogsErr struct {
	logger *slog.Logger
}

// NewLogsErr instantiate new LogsErr for given structured logger.
func NewLogsErr(logger *slog.Logger) *LogsErr {
	return &LogsErr{logger: logger}
}

// Send sends given message as a log of severity ERROR.
func (l *LogsErr) Send(_ context.Context, msg Message) error {
	const prefix = "[NOTIFICATION]"
	text := fmt.Sprintf("%s [%s][%s][%s]: %s", prefix, msg.DagId, msg.ExecTs,
		msg.TaskIdOrEmpty(), msg.Body)
	l.logger.Error(text)
	return nil
}
