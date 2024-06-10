// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package notify provides a way to send external notifications.
package notify

import "context"

// Sender sends a Message notification. Usually onto an external channel of
// communication.
type Sender interface {
	Send(context.Context, Message) error
}

// Message contains a message body text and DAG run contextual information.
type Message struct {
	DagId  string
	ExecTs string
	TaskId *string
	Body   string
}

// Returns TaskId if *TaskId is not nil, else it return empty string.
func (m Message) TaskIdOrEmpty() string {
	if m.TaskId == nil {
		return ""
	}
	return *m.TaskId
}
