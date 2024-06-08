package notify

import (
	"context"
	"fmt"
)

// Mock sends message into a string slice in memory. It implements Sender
// interface. Useful mostly for testing.
type Mock struct {
	buf *[]string
}

// NewMock initialized Mock for given string slice buffor.
func NewMock(buffor *[]string) *Mock {
	return &Mock{buf: buffor}
}

// Send sends a message onto internal Mock buffor.
func (m *Mock) Send(_ context.Context, msg Message) error {
	taskId := ""
	if msg.TaskId != nil {
		taskId = *msg.TaskId
	}
	text := fmt.Sprintf("%s %s %s: %s", msg.DagId, msg.ExecTs, taskId, msg.Body)
	*m.buf = append(*m.buf, text)
	return nil
}
