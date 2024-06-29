// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package notify provides a way to send external notifications.
package notify

import (
	"context"
	"io"
)

// Template represents a message template. Go standard text/template.Template
// and html/template.Template satisfy this interface.
type Template interface {
	Execute(io.Writer, any) error
}

// Sender sends a Message notification. Usually onto an external channel of
// communication. Template should be already parsed text template which can use
// additional information from MsgData.
type Sender interface {
	Send(context.Context, Template, MsgData) error
}

// Message contains a DAG run contextual information.
type MsgData struct {
	DagId        string
	ExecTs       string
	TaskId       *string
	TaskRunError error
	RuntimeInfo  map[string]any
}
