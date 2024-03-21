// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package schedule contains implementations of ppacer Schedules.
package schedule

import "time"

// Schedule represents process' schedule. Start says when schedule starts.
// Next method for given time and possibly time of the latest run determines
// when the next schedule should happen. String method should provide
// serialization to store schedule definition in the database.
type Schedule interface {
	Start() time.Time
	Next(time.Time, *time.Time) time.Time
	String() string
}
