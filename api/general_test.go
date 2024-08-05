package api

import (
	"testing"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestToTimstamp(t *testing.T) {
	now := time.Now()
	t1 := time.Date(
		now.Year(), now.Month(), now.Day(), 13, 13, 13, 131000000,
		time.UTC,
	)
	yestarday := t1.Add(-24 * time.Hour)
	yDate := time.Date(
		yestarday.Year(), yestarday.Month(), yestarday.Day(), 0, 0, 0, 0,
		time.UTC,
	)
	yDateStr := yDate.Format(timeutils.UiDateFormat)

	tests := []struct {
		input    time.Time
		expected Timestamp
	}{
		{
			t1,
			Timestamp{
				ToDisplay: "13:13:13",
				Date:      t1.Format(timeutils.UiDateFormat),
				Time:      "13:13:13.131",
				Timezone:  "UTC",
			},
		},
		{
			yestarday,
			Timestamp{
				ToDisplay: yDateStr + " " + "13:13:13",
				Date:      yDateStr,
				Time:      "13:13:13.131",
				Timezone:  "UTC",
			},
		},
	}

	for _, test := range tests {
		res := ToTimestamp(test.input)
		if res != test.expected {
			t.Errorf("Expected %v, got: %v", test.expected, res)
		}
	}
}

func BenchmarkToTimestampToday(b *testing.B) {
	now := time.Now()
	for i := 0; i < b.N; i++ {
		ToTimestamp(now)
	}
}

func BenchmarkToTimestampOtherday(b *testing.B) {
	yestarday := time.Now().Add(-24 * time.Hour)
	for i := 0; i < b.N; i++ {
		ToTimestamp(yestarday)
	}
}
