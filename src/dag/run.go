package dag

import "fmt"

// RunStatus enumerates possible DAG run states.
type RunStatus int

const (
	RunReadyToSchedule RunStatus = iota
	RunScheduled
	RunRunning
	RunSuccess
	RunFailed
)

// String serialize RunStatus.
func (s RunStatus) String() string {
	return [...]string{
		"READY_TO_SCHEDULE",
		"SCHEDULED",
		"RUNNING",
		"SUCCESS",
		"FAILED",
	}[s]
}

// ParseRunStatus parses run status based on given string. If given string does
// not match any run status, then non-nil error is returned. Statuses are
// case-sensitive.
func ParseRunStatus(s string) (RunStatus, error) {
	states := map[string]RunStatus{
		"READY_TO_SCHEDULE": RunReadyToSchedule,
		"SCHEDULED":         RunScheduled,
		"RUNNING":           RunRunning,
		"SUCCESS":           RunSuccess,
		"FAILED":            RunFailed,
	}
	if status, ok := states[s]; ok {
		return status, nil
	}
	return 0, fmt.Errorf("invalid RunStatus: %s", s)
}
