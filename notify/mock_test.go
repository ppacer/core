package notify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestMockSend(t *testing.T) {
	ctx := context.Background()
	const dagId = "sample_dag"
	const execTs = "2024-06-08 08:56:00"

	msgs := make([]string, 0, 10)
	sender := NewMock(&msgs)
	task1 := "task1"
	tmpl := MockTemplate("mock")

	inputs := []struct {
		taskId *string
	}{
		{nil},
		{&task1},
	}

	for _, input := range inputs {
		msgCtx := MsgData{
			DagId:  dagId,
			ExecTs: execTs,
			TaskId: input.taskId,
		}
		sErr := sender.Send(ctx, tmpl, msgCtx)
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
		var msgBytes bytes.Buffer
		msgCtx := MsgData{
			DagId:  dagId,
			ExecTs: execTs,
			TaskId: input.taskId,
		}
		writeErr := tmpl.Execute(&msgBytes, msgCtx)
		if writeErr != nil {
			t.Errorf("Error while executing template: %s", writeErr.Error())
		}
		if msgSent != msgBytes.String() {
			t.Errorf("For message %d expected sent message [%s], but got [%s]",
				idx, msgBytes.String(), msgSent)
		}
	}
}

func TestMockSendManyTmpl(t *testing.T) {
	ctx := context.Background()

	msgs := make([]string, 0, 10)
	sender := NewMock(&msgs)

	inputs := []struct {
		tmplStr  string
		data     MsgData
		expected string
	}{
		{"CONST MOCK", MsgData{}, "CONST MOCK"},
		{
			"Hello from {{.DagId}}",
			MsgData{DagId: "my_dag"},
			"Hello from my_dag",
		},
		{
			"{{.DagId}}|{{.ExecTs}}",
			MsgData{DagId: "my_dag", ExecTs: "2024-06-24"},
			"my_dag|2024-06-24",
		},
		{
			`
				Alert for {{.DagId}} at {{.ExecTs}}
			`,
			MsgData{DagId: "my_dag", ExecTs: "2024-06-24"},
			`
				Alert for my_dag at 2024-06-24
			`,
		},
		{
			`
				Alert for {{.DagId}} at {{.ExecTs}}
				{{- if .TaskRunError}}
					Error: {{.TaskRunError.Error}}
				{{end}}
			`,
			MsgData{DagId: "my_dag", ExecTs: "2024-06-24"},
			`
				Alert for my_dag at 2024-06-24
			`,
		},
		{
			`
				Alert for {{.DagId}} at {{.ExecTs}}
				{{- if .TaskRunError}}
					Error: {{.TaskRunError.Error}}
				{{- end}}
			`,
			MsgData{
				DagId:        "my_dag",
				ExecTs:       "2024-06-24",
				TaskRunError: errors.New("ops!"),
			},
			`
				Alert for my_dag at 2024-06-24
					Error: ops!
			`,
		},
	}

	for _, input := range inputs {
		tmpl, parseErr := template.New("tmp").Parse(input.tmplStr)
		if parseErr != nil {
			t.Errorf("Error while parsing template [%s]: %s", input.tmplStr,
				parseErr.Error())
		}
		sErr := sender.Send(ctx, tmpl, input.data)
		if sErr != nil {
			t.Errorf("Error while sending a message: %s", sErr.Error())
		}
	}

	if len(msgs) != len(inputs) {
		t.Errorf("Expected %d messages sent, but got: %d", len(inputs),
			len(msgs))
	}

	for idx, input := range inputs {
		if input.expected != msgs[idx] {
			t.Errorf("For message %d, expected [%s], but got [%s]",
				idx, input.expected, msgs[idx])
		}
	}
}

func TestMockTemplateNoErr(t *testing.T) {
	taskId := "task_1"
	data := MsgData{
		DagId:  "sample_dag",
		ExecTs: timeutils.ToString(time.Now()),
		TaskId: &taskId,
	}
	tmpl := MockTemplate("mock")

	expected := fmt.Sprintf(`
[%s] [%s] [%s]:
	Mock message!
`, data.DagId, data.ExecTs, *data.TaskId)

	var msgBytes bytes.Buffer
	writeErr := tmpl.Execute(&msgBytes, data)
	if writeErr != nil {
		t.Errorf("Error while executing the template: %s", writeErr.Error())
	}

	expectedNospace := strings.TrimSpace(expected)
	msgBytesNospace := strings.TrimSpace(msgBytes.String())

	if expectedNospace != msgBytesNospace {
		t.Errorf("Expected %s, but got: %s", expectedNospace, msgBytesNospace)
	}
}

func TestMockTemplateWithErr(t *testing.T) {
	taskId := "task_1"
	data := MsgData{
		DagId:        "sample_dag",
		ExecTs:       timeutils.ToString(time.Now()),
		TaskId:       &taskId,
		TaskRunError: errors.New("task failed"),
	}
	tmpl := MockTemplate("mock")

	expected := fmt.Sprintf(`
[%s] [%s] [%s]:
	Mock message!
	Got error: %s
`, data.DagId, data.ExecTs, *data.TaskId, data.TaskRunError.Error())

	var msgBytes bytes.Buffer
	writeErr := tmpl.Execute(&msgBytes, data)
	if writeErr != nil {
		t.Errorf("Error while executing the template: %s", writeErr.Error())
	}

	expectedNospace := strings.TrimSpace(expected)
	msgBytesNospace := strings.TrimSpace(msgBytes.String())

	if expectedNospace != msgBytesNospace {
		t.Errorf("Expected %s, but got: %s", expectedNospace, msgBytesNospace)
	}
}
