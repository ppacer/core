package dag

import (
	"fmt"
	"time"
)

// Schedule represents process' schedule.
type Schedule interface {
	StartTime() time.Time
	NextExecTime(time.Time) time.Time
	String() string
}

// IntervalSchedule is a schedule with ticks every Interval interval.
type IntervalSchedule struct {
	Start    time.Time
	Interval time.Duration
}

func (is IntervalSchedule) StartTime() time.Time {
	return is.Start
}

func (is IntervalSchedule) NextExecTime(baseTime time.Time) time.Time {
	return baseTime.Add(is.Interval)
}

func (is IntervalSchedule) String() string {
	return fmt.Sprintf("IntervalSchedule: %s", is.Interval)
}
