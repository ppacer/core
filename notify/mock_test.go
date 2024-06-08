package notify

import (
	"context"
	"fmt"
	"testing"
)

func TestMockSend(t *testing.T) {
	ctx := context.Background()
	const dagId = "sample_dag"
	const execTs = "2024-06-08 08:56:00"

	msgs := make([]string, 0, 10)
	sender := NewMock(&msgs)
	task1 := "task1"

	inputs := []struct {
		taskId *string
		body   string
	}{
		{nil, "test message"},
		{&task1, "test error"},
	}

	for _, input := range inputs {
		msg := Message{
			DagId:  dagId,
			ExecTs: execTs,
			TaskId: input.taskId,
			Body:   input.body,
		}
		sErr := sender.Send(ctx, msg)
		if sErr != nil {
			t.Errorf("Error while sending a message: %s", sErr.Error())
		}
	}

	if len(msgs) != len(inputs) {
		t.Errorf("Expected %d messages sent, but got: %d", len(inputs),
			len(msgs))
	}

	for idx := 0; idx < len(inputs); idx++ {
		msgSent := msgs[idx]
		input := inputs[idx]
		taskId := ""
		if input.taskId != nil {
			taskId = *input.taskId
		}
		expectedMsg := fmt.Sprintf("%s %s %s: %s", dagId, execTs, taskId,
			input.body)
		if msgSent != expectedMsg {
			t.Errorf("For message %d expected sent message [%s], but got [%s]",
				idx, expectedMsg, msgSent)
		}
	}
}
