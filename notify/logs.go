package notify

import (
	"bytes"
	"context"
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
func (l *LogsErr) Send(_ context.Context, tmpl Template, data MsgData) error {
	var msgBuff bytes.Buffer
	writeErr := tmpl.Execute(&msgBuff, data)
	if writeErr != nil {
		return writeErr
	}
	l.logger.Error(msgBuff.String())
	return nil
}
