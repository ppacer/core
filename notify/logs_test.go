package notify

import (
	"bytes"
	"context"
	"errors"
	htmltmpl "html/template"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"text/template"
)

func TestLogsErrSimple(t *testing.T) {
	const dagId = "sample_dag"
	const execTs = "2024-06-08 22:10:00"
	task1 := "task1"
	ctx := context.Background()

	var buffor bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffor, nil))
	logsErr := NewLogsErr(logger)
	tmpl := MockTemplate("mock")

	inputs := []struct {
		taskId *string
	}{
		{&task1},
		{nil},
	}

	for _, input := range inputs {
		err := logsErr.Send(ctx, tmpl, MsgData{
			DagId:  dagId,
			ExecTs: execTs,
			TaskId: input.taskId,
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

	for idx := range inputs {
		if !strings.Contains(logsOutLines[idx], "ERR") {
			t.Errorf("Expected ERR severity in log notification [%s], but is not",
				logsOutLines[idx])
		}
	}
}

func TestLogsManyTmpl(t *testing.T) {
	ctx := context.Background()

	var buffor bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffor, nil))
	logsErr := NewLogsErr(logger)

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
			`Alert for my_dag at 2024-06-24`,
		},
		{
			`
				Alert for {{.DagId}} at {{.ExecTs}}
				{{- if .TaskRunError}}
					Error: {{.TaskRunError.Error}}
				{{end}}
			`,
			MsgData{DagId: "my_dag", ExecTs: "2024-06-24"},
			`Alert for my_dag at 2024-06-24`,
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
			`Error: ops!`,
		},
	}

	for _, input := range inputs {
		tmpl, parseErr := template.New("tmp").Parse(input.tmplStr)
		if parseErr != nil {
			t.Errorf("Error while parsing template [%s]: %s", input.tmplStr,
				parseErr.Error())
		}
		sErr := logsErr.Send(ctx, tmpl, input.data)
		if sErr != nil {
			t.Errorf("Error while sending a message: %s", sErr.Error())
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
		phrase := strings.TrimSpace(input.expected)
		if !strings.Contains(logsOutLines[idx], phrase) {
			t.Errorf("Expected [%s] in log notification [%s] (line %d), but it's not",
				phrase, logsOutLines[idx], idx)
		}
	}
}

func TestLogsManyHTMLTmpl(t *testing.T) {
	ctx := context.Background()

	var buffor bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffor, nil))
	logsErr := NewLogsErr(logger)

	inputs := []struct {
		tmplStr  string
		data     MsgData
		expected string
	}{
		{"CONST MOCK", MsgData{}, "CONST MOCK"},
		{
			"<h1>{{.DagId}}</h1>",
			MsgData{DagId: "my_dag"},
			"<h1>my_dag</h1>",
		},
		{
			"<div class='X'><p>{{.DagId}}</p></div>",
			MsgData{DagId: "my_dag"},
			"<div class='X'><p>my_dag</p></div>",
		},
	}

	for _, input := range inputs {
		tmpl, parseErr := htmltmpl.New("tmp").Parse(input.tmplStr)
		if parseErr != nil {
			t.Errorf("Error while parsing template [%s]: %s", input.tmplStr,
				parseErr.Error())
		}
		sErr := logsErr.Send(ctx, tmpl, input.data)
		if sErr != nil {
			t.Errorf("Error while sending a message: %s", sErr.Error())
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
		phrase := strings.TrimSpace(input.expected)
		if !strings.Contains(logsOutLines[idx], phrase) {
			t.Errorf("Expected [%s] in log notification [%s] (line %d), but it's not",
				phrase, logsOutLines[idx], idx)
		}
	}
}
