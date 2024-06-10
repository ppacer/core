package notify

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
)

func TestLogsErrSimple(t *testing.T) {
	const dagId = "sample_dag"
	const execTs = "2024-06-08 22:10:00"
	task1 := "task1"
	ctx := context.Background()

	var buffor bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffor, nil))
	logsErr := NewLogsErr(logger)

	inputs := []struct {
		taskId *string
		msg    string
	}{
		{&task1, "test msg"},
		{nil, "test msg for nil"},
		{nil, ""},
		{&task1, ""},
	}

	for _, input := range inputs {
		err := logsErr.Send(ctx, Message{
			DagId:  dagId,
			ExecTs: execTs,
			TaskId: input.taskId,
			Body:   input.msg,
		})
		if err != nil {
			t.Errorf("Error while sending notification for [%v]: %s",
				input, err.Error())
		}
	}

	logsOutLines := strings.Split(buffor.String(), "\n")
	logsOutLines = slices.DeleteFunc(logsOutLines, func(s string) bool {
		return len(s) == 0 // remove empty lines (should be one at the end)
	})
	if len(logsOutLines) != len(inputs) {
		t.Errorf("Expected %d notifications, got: %d",
			len(inputs), len(logsOutLines))
	}

	for idx, input := range inputs {
		if !strings.Contains(logsOutLines[idx], "ERR") {
			t.Errorf("Expected ERR severity in log notification [%s], but is not",
				logsOutLines[idx])
		}
		if !strings.Contains(logsOutLines[idx], input.msg) {
			t.Errorf("Expected [%s] to be substring of notification [%s], but is not",
				input.msg, logsOutLines[idx])
		}
	}
}
