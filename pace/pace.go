// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// Package pace provides strategies for controlling the pace of events.
//
// The pace package defines an interface for strategies that control the
// interval between events, allowing for adaptive pacing based on dynamic
// conditions. This can be useful for scenarios such as adaptive polling,
// rate limiting, or other forms of throttling.
package pace

import "time"

// Strategy defines an interface for adaptive pacing strategies.
//
// Implementations of the Strategy interface are used to control the interval
// between events in a dynamic and adaptable manner. This can be particularly
// useful in scenarios such as adaptive polling, rate limiting, and other
// forms of event pacing where the timing between events needs to be adjusted
// based on specific conditions.
//
// NextInterval method returns the duration to wait before the next event
// should occur. The interval can adapt based on the specific implementation's
// logic, allowing for flexible pacing strategies.
//
// Reset resets the pacing strategy to its initial state. This is typically
// called when an event of interest occurs, and the pacing strategy needs to
// restart its interval calculation from the beginning.
type Strategy interface {
	NextInterval() time.Duration
	Reset()
}
