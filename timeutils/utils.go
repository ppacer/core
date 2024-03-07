// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package timeutils

import (
	"math/rand"
	"time"
)

const LOG_PREFIX = "timeutils"

// Timestamp format for time.Time serialization and deserialization. This
// format is used to store timestamps in the database.
const TimestampFormat = "2006-01-02T15:04:05.999999MST-07:00"

// Date format for time.Time serialization and deserialization.
const DateFormat = "2006-01-02"

// ToString serialize give time.Time to string based on TimestampFormat format.
func ToString(t time.Time) string {
	return t.Format(TimestampFormat)
}

// ToDateUTCString move given time.Time to UTC location and serialize it to
// date string based on DateFormat format.
func ToDateUTCString(t time.Time) string {
	return t.UTC().Format(DateFormat)
}

// FromString tries to recreate time.Time based on given string value according
// to TimestampFormat format.
func FromString(s string) (time.Time, error) {
	return time.Parse(TimestampFormat, s)
}

// In most cases FromString should be called on strings created by ToString and
// should succeed. In cases when we are pretty sure that FromString will
// succeed, we can use FromStringMust. If FromString would fail for given
// input, error would be logged and time.Time{} would be returned.
func FromStringMust(s string) time.Time {
	t, err := FromString(s)
	if err != nil {
		// TODO(dskrzypiec): should we panic in this case?
		return time.Time{}
	}
	return t
}

func RandomUtcTime(minYear int) time.Time {
	year := rand.Intn(2023-minYear) + minYear
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1

	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	ns := rand.Intn(10000000) * 1000

	return time.Date(year, time.Month(month), day, hour, minute, second, ns,
		time.UTC)
}
