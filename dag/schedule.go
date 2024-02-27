// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package dag

import (
	"fmt"
	"time"
)

// Schedule represents process' schedule. StartTime says when schedule starts.
// Next method for given time and possibly time of the latest run determines
// when the next schedule should happen. String method should provide
// serialization to store schedule definition in the database.
type Schedule interface {
	StartTime() time.Time
	Next(time.Time, *time.Time) time.Time
	String() string
}

// FixedSchedule is a schedule with ticks every Interval interval since Start.
type FixedSchedule struct {
	Start    time.Time
	Interval time.Duration
}

// StartTime returns Start.
func (is FixedSchedule) StartTime() time.Time {
	return is.Start
}

// Next determines next schedule time. Usually it's interval later then the
// previous schedule point. When given currentTime is before Start, then Start
// is returned as next schedule. In case when there was not previous schedule
// yet and currentTime is after Start, then the next schedule would be the
// first time obtained by adding Interval to Start which is after currentTime.
func (is FixedSchedule) Next(currentTime time.Time, prevSchedule *time.Time) time.Time {
	if currentTime.Before(is.Start) && prevSchedule == nil {
		return is.Start
	}
	if prevSchedule != nil && currentTime.After(*prevSchedule) {
		return (*prevSchedule).Add(is.Interval)
	}
	// The following might be very slow, but would be run at most one time per
	// DAG.
	ts := is.Start
	for {
		if currentTime.Before(ts) {
			return ts
		}
		ts = ts.Add(is.Interval)
	}
}

// String returns serialized representation of the schedule.
func (is FixedSchedule) String() string {
	return fmt.Sprintf("FixedSchedule: %s", is.Interval)
}
