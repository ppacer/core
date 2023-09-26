package timeutils

import "time"

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
