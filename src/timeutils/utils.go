package timeutils

import (
	"time"

	"github.com/rs/zerolog/log"
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

// In most cases FromString should be called on strings created by ToString and should succeed. In cases when we are
// pretty sure that FromString will succeed, we can use FromStringMust. If FromString would fail for given input, error
// would be logged and time.Time{} would be returned.
func FromStringMust(s string) time.Time {
	t, err := FromString(s)
	if err != nil {
		log.Error().Str("timestamp", s).Msgf("[%s] Cannot deserialize to time.Time based on format %s",
			LOG_PREFIX, TimestampFormat)
		return time.Time{}
	}
	return t
}
