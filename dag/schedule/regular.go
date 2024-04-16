// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package schedule

import "time"

// Daily returns "m h * * *" cron.
func Daily(atHour, atMinute int) *Cron {
	return NewCron().AtMinute(atMinute).AtHour(atHour)
}

// Weekly return "m h * * w" cron.
func Weekly(weekday time.Weekday, hour, minute int) *Cron {
	return NewCron().
		AtMinute(minute).
		AtHour(hour).
		OnWeekday(weekday)
}

// Monthly returns "m h d * *" cron.
func Monthly(dom, hour, minute int) *Cron {
	return NewCron().
		AtMinute(minute).
		AtHour(hour).
		OnMonthDay(dom)
}

// Yearly return "m h d m *" cron.
func Yearly(month time.Month, dom, hour, minute int) *Cron {
	return NewCron().
		AtMinute(minute).
		AtHour(hour).
		OnMonthDay(dom).
		InMonth(month)
}
