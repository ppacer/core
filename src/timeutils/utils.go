package timeutils

import (
	"log/slog"
	"math/rand"
	"time"
)

const LOG_PREFIX = "timeutils"

// Timestamp format for time.Time serialization and deserialization. This
// format is used to store timestamps in the database.
const TimestampFormat = "2006-01-02T15:04:05.999999-07:00"

// ToString serialize give time.Time to string based on TimestampFormat format.
func ToString(t time.Time) string {
	return t.Format(TimestampFormat)
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
		slog.Error("Cannot deserialize to time.Time", "timestamp", s, "format",
			TimestampFormat)
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
