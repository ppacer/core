package schedule

import (
	"fmt"
	"time"
)

// Fixed is a schedule with ticks every Interval interval since Start.
type Fixed struct {
	start    time.Time
	interval time.Duration
}

// NewFixed creates Fixed schedule which starts at startTs and ticks every
// interval.
func NewFixed(startTs time.Time, interval time.Duration) Fixed {
	return Fixed{
		start:    startTs,
		interval: interval,
	}
}

// Start returns schedule start time.
func (is Fixed) Start() time.Time {
	return is.start
}

// Next determines next schedule time. Usually it's interval later then the
// previous schedule point. When given currentTime is before Start, then Start
// is returned as next schedule. In case when there was not previous schedule
// yet and currentTime is after Start, then the next schedule would be the
// first time obtained by adding Interval to Start which is after currentTime.
func (is Fixed) Next(currentTime time.Time, prevSchedule *time.Time) time.Time {
	if currentTime.Before(is.start) && prevSchedule == nil {
		return is.start
	}
	if prevSchedule != nil && currentTime.After(*prevSchedule) {
		return (*prevSchedule).Add(is.interval)
	}
	// The following might be very slow, but would be run at most one time per
	// DAG.
	ts := is.start
	for {
		if currentTime.Before(ts) {
			return ts
		}
		ts = ts.Add(is.interval)
	}
}

// String returns serialized representation of the schedule.
func (is Fixed) String() string {
	return fmt.Sprintf("Fixed: %s", is.interval)
}
