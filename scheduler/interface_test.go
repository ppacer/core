package scheduler

import "testing"

func TestClientInterface(t *testing.T) {
	// We just want to ensure that scheduler.Client satisfies
	// scheduler.Interface interface.
	var _ API = NewClient("", nil, nil, DefaultClientConfig)
}
