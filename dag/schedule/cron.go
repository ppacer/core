// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package schedule

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Cron represents classic cron schedule expression. It implements Schedule
// interface.
//
// Usually cron schedule can be initialized using provided fluent API. For
// example if you want set '5,45 10 * * 1" cron schedule, you can write:
//
//	cronSched := NewCron().AtMinutes(5, 45).AtHour(10).OnWeekday(time.Monday)
//
// By default schedule starts at the Unix epoch start (1970-01-01). It can be
// changed using Starts method.
type Cron struct {
	start      time.Time
	minute     []int
	hour       []int
	dayOfMonth []int
	month      []int
	dayOfWeek  []int
}

// NewCron initialize new default Cron which is "* * * * *" and starts at
// 1970-01-01.
func NewCron() *Cron {
	return &Cron{start: time.Unix(0, 0)}
}

// Starts set Cron start time.
func (c *Cron) Starts(start time.Time) *Cron {
	c.start = start
	return c
}

// Start for cron schedule returns always 1970-01-01.
func (c *Cron) Start() time.Time { return time.Unix(0, 0) }

// Next computes the next time according to set cron schedule, after given
// currentTime. In case when curretTime is precisely on cron schedule (eg,
// 2024-04-02 12:00:00 for "0 12 * * *" cron), then next cron schedule is
// returned (2024-04-03 12:00:00 for mentioned example).
func (c *Cron) Next(currentTime time.Time, _ *time.Time) time.Time {
	next := zeroSecondsAndSubs(currentTime)
	next = c.setMinutes(next)
	next = c.setHours(next)
	next = c.setDayOfMonth(next)
	// TODO: include months and weekdays.
	return next
}

// String returns cron schedule string expression.
func (c *Cron) String() string {
	var parts [5]string
	parts[0] = cronPartToString(c.minute)
	parts[1] = cronPartToString(c.hour)
	parts[2] = cronPartToString(c.dayOfMonth)
	parts[3] = cronPartToString(c.month)
	parts[4] = cronPartToString(c.dayOfWeek)
	return strings.Join(parts[:], " ")
}

// Sets minute part in cron schedule. Given input should be from interval
// [0,59].
func (c *Cron) AtMinute(m int) *Cron {
	c.minute = []int{m % 60}
	return c
}

// Sets minute part in cron schedule. Each input should be from interval [0,
// 59].
func (c *Cron) AtMinutes(minutes ...int) *Cron {
	m := make([]int, len(minutes))
	for idx, minute := range minutes {
		m[idx] = minute % 60
	}
	sort.Ints(m)
	c.minute = m
	return c
}

// Sets hour part in cron schedule. Given input should be from interval [0,
// 23].
func (c *Cron) AtHour(h int) *Cron {
	c.hour = []int{h % 24}
	return c
}

// Sets hour part in cron schedule. Each input should be from interval [0, 23].
func (c *Cron) AtHours(hours ...int) *Cron {
	h := make([]int, len(hours))
	for idx, hour := range hours {
		h[idx] = hour % 24
	}
	sort.Ints(h)
	c.hour = h
	return c
}

// Sets weekday part in cron schedule.
func (c *Cron) OnWeekday(d time.Weekday) *Cron {
	c.dayOfWeek = []int{int(d) % 7}
	return c
}

// Sets weekday part in cron schedule.
func (c *Cron) OnWeekdays(days ...time.Weekday) *Cron {
	dInts := make([]int, len(days))
	for idx, day := range days {
		dInts[idx] = int(day) % 7
	}
	sort.Ints(dInts)
	c.dayOfWeek = dInts
	return c
}

// Sets day of month part in cron schedule. Given input should be from interval
// [1, 31].
func (c *Cron) OnMonthDay(monthDay int) *Cron {
	c.dayOfMonth = []int{monthDay % 32}
	return c
}

// Sets day of month part in cron schedule. Each input should be from interval
// [1, 31].
func (c *Cron) OnMonthDays(monthDays ...int) *Cron {
	mdInts := make([]int, len(monthDays))
	for idx, day := range monthDays {
		mdInts[idx] = day % 32
	}
	sort.Ints(mdInts)
	c.dayOfMonth = mdInts
	return c
}

// Checks if c is default cron instance - "* * * * *"
func (c *Cron) isDefault() bool {
	return len(c.minute) == 0 &&
		len(c.hour) == 0 &&
		len(c.dayOfMonth) == 0 &&
		len(c.month) == 0 &&
		len(c.dayOfWeek) == 0
}

func cronPartToString(part []int) string {
	if len(part) == 0 {
		return "*"
	}
	var str []string
	for _, num := range part {
		str = append(str, strconv.Itoa(int(num)))
	}
	return strings.Join(str, ",")
}

func (c *Cron) setMinutes(t time.Time) time.Time {
	minutesSet, nextMinute := findNextInt(c.minute, t.Minute(), false)
	if !minutesSet {
		// regular * case
		return t.Add(time.Minute)
	}
	if nextMinute > 0 {
		// Another minute in the current hour
		return setMinute(t, nextMinute)
	}
	// in this case we need to increase hour and set minutes
	return setMinute(t.Add(time.Hour), c.minute[0])
}

func (c *Cron) setHours(t time.Time) time.Time {
	hoursSet, nextHour := findNextInt(c.hour, t.Hour(), true)
	if !hoursSet {
		// regular * case which is handled in setMinutes
		return t
	}
	if nextHour == t.Hour() {
		return t
	}
	if nextHour > 0 {
		// another hour in the current day
		if len(c.minute) == 0 {
			// * H ... case
			return setMinute(setHour(t, nextHour), 0)
		}
		return setMinute(setHour(t, nextHour), c.minute[0])
	}
	// in this case we need to increase day and set hour
	if len(c.minute) == 0 {
		// * H ... case
		return setMinute(setHour(t.Add(24*time.Hour), c.hour[0]), 0)
	}
	return setMinute(setHour(t.Add(24*time.Hour), c.hour[0]), c.minute[0])
}

func (c *Cron) setDayOfMonth(t time.Time) time.Time {
	domSet, nextDom := findNextInt(c.dayOfMonth, t.Day(), true)
	if !domSet {
		// regular * case which is handled in setHours
		return t
	}
	if nextDom == t.Day() {
		return t
	}
	if nextDom > 0 {
		return setDay(c.setHourAndMinuteForNewDay(t), nextDom)
	}
	t = incrementMonth(t)
	return setDay(c.setHourAndMinuteForNewDay(t), c.dayOfMonth[0])
}

func (c *Cron) setHourAndMinuteForNewDay(t time.Time) time.Time {
	if len(c.hour) == 0 {
		t = setHour(t, 0)
	} else {
		t = setHour(t, c.hour[0])
	}
	if len(c.minute) == 0 {
		t = setMinute(t, 0)
	} else {
		t = setMinute(t, c.minute[0])
	}
	return t
}

// Helper function to increment the month and reset day, hour, and minute.
func incrementMonth(t time.Time) time.Time {
	newMonth := t.Month() + 1
	newYear := t.Year()
	if newMonth > 12 {
		newMonth = 1
		newYear++
	}
	return time.Date(newYear, newMonth, 1, 0, 0, 0, 0, t.Location())
}

func zeroSecondsAndSubs(t time.Time) time.Time {
	return time.Date(
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location(),
	)
}

func findNextInt(sorted []int, current int, include bool) (bool, int) {
	if len(sorted) == 0 {
		return false, -1
	}
	for _, v := range sorted {
		if include && v >= current {
			return true, v
		}
		if !include && v > current {
			return true, v
		}
	}
	return true, -1
}

func setDay(t time.Time, day int) time.Time {
	const maxAttempts = 13
	year := t.Year()
	month := t.Month()

	for i := 0; i < maxAttempts; i++ {
		candidate := fmt.Sprintf("%d-%02d-%02d", year, month, day)
		newDate, err := time.Parse("2006-01-02", candidate)
		if err == nil && newDate.Day() == day {
			newTime := time.Date(
				newDate.Year(), newDate.Month(), newDate.Day(), t.Hour(),
				t.Minute(), t.Second(), t.Nanosecond(), t.Location(),
			)
			// We need to check hour and minute for the new date and time,
			// because it might be shifted in some cases (like day light saving
			// changes).
			if t.Hour() == newTime.Hour() && t.Minute() == newTime.Minute() {
				return newTime
			}
		}
		// New day is invalid for the current month (eg 30th for February), we
		// new to move to the next month.
		month++
		if month > 12 {
			month = 1
			year++
		}
	}
	return time.Time{} // shouldn't ever happen
}

func setHour(t time.Time, hour int) time.Time {
	return time.Date(
		t.Year(), t.Month(), t.Day(), hour, t.Minute(), t.Second(),
		t.Nanosecond(), t.Location(),
	)
}

func setMinute(t time.Time, minute int) time.Time {
	return time.Date(
		t.Year(), t.Month(), t.Day(), t.Hour(), minute, t.Second(),
		t.Nanosecond(), t.Location(),
	)
}
