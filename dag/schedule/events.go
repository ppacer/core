package schedule

import "fmt"

// Event represents possible events regarding schedule points.
type Event int

const (
	Regular Event = iota
	CaughtUp
	Skipped
	OutOfBand
)

// String serialize Event.
func (e Event) String() string {
	return [...]string{
		"REGULAR",
		"CAUGHT_UP",
		"SKIPPED",
		"OUT_OF_BAND",
	}[e]
}

// ParseEvent parses Event based on given string. Events are case-sensitive.
func ParseEvent(s string) (Event, error) {
	events := map[string]Event{
		"REGULAR":     Regular,
		"CAUGHT_UP":   CaughtUp,
		"SKIPPED":     Skipped,
		"OUT_OF_BAND": OutOfBand,
	}
	if event, ok := events[s]; ok {
		return event, nil
	}
	return 0, fmt.Errorf("invalid schedule Event: %s", s)
}
