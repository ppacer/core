package api

import (
	"time"

	"github.com/ppacer/core/timeutils"
)

// Timestamp represents information about timestamp. ToDisplay is the field
// which will be displayed on the UI as the main information. Other details,
// like timezone might be used in tooltips.
type Timestamp struct {
	ToDisplay string `json:"toDisplay"`
	Date      string `json:"date"`
	Time      string `json:"time"`
	Timezone  string `json:"timezone"`
}

// ToTimestamp converts given time into UI Timestamp information.
func ToTimestamp(t time.Time) Timestamp {
	timezoneName, _ := t.Zone()
	return Timestamp{
		ToDisplay: timeutils.ToStringUI(t),
		Date:      t.Format(timeutils.UiDateFormat),
		Time:      t.Format(timeutils.UiTimeDetailedFormat),
		Timezone:  timezoneName,
	}
}
