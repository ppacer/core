package dag

import (
	"fmt"
	"time"
)

// Schedule represents process' schedule.
type Schedule interface {
	StartTime() time.Time
	Next(time.Time) time.Time
	String() string
}

// FixedSchedule is a schedule with ticks every Interval interval.
type FixedSchedule struct {
	Start    time.Time
	Interval time.Duration
}

func (is FixedSchedule) StartTime() time.Time {
	return is.Start
}

func (is FixedSchedule) Next(baseTime time.Time) time.Time {
	if baseTime.Compare(is.Start) == -1 {
		return is.Start
	}
	ts := is.Start
	for {
		// TODO(dskrzypiec): This algorithm can and should be improved regarding performance. It's good enough for
		// first sketch but should be done propely eventually.
		if baseTime.Compare(ts) == -1 {
			return ts
		}
		ts = ts.Add(is.Interval)
	}
}

func (is FixedSchedule) String() string {
	return fmt.Sprintf("FixedSchedule: %s", is.Interval)
}
