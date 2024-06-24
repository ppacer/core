package notify

import (
	"bytes"
	"context"
	"text/template"
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
func (m *Mock) Send(_ context.Context, tmpl *template.Template, data MsgData) error {
	var msgBuff bytes.Buffer
	writeErr := tmpl.Execute(&msgBuff, data)
	if writeErr != nil {
		return writeErr
	}
	*m.buf = append(*m.buf, msgBuff.String())
	return nil
}

// MockTemplate returns parsed text template to be used in Mock notifier,
// mainly for testing.
func MockTemplate(tmplName string) *template.Template {
	def := `
[{{.DagId}}] [{{.ExecTs}}] [{{.TaskId}}]:
	Mock message!
{{- if .TaskRunError}}
	Got error: {{.TaskRunError.Error}}
{{end}}
`
	return template.Must(template.New(tmplName).Parse(def))
}
