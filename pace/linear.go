// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package pace

import (
	"errors"
	"fmt"
	"time"
)

// LinearBackoff implements Strategy for linear back off with possible
// repetitions. Intervals starts at min, are repeated repeat times, then
// increases duration to (min + step) and are again repeated repeat times.
// Those increments continues until we reach max duration. Intervals stays at
// max duration until the next Reset() call.
//
// Example for min=1ms, max=1s (1000ms), step=250ms and repeat=2.
// Consecutive calls to NextInterval() would return the following sequence:
//
//	1ms, 1ms, 251ms, 251ms, 501ms, 501ms, 1000ms, 1000ms, ... (until Reset),
//	1000ms
//
// For more gradual increases (min=0ms, max=1s, step=10ms and repeat=1):
//
//	0ms, 10ms, 20ms, 30ms, ..., 980ms, 990ms, 1000ms, 1000ms, 1000ms, ...
//
// Cumulative duration from the beginning to reach the first max value (1s)
// takes around 50 seconds.
type LinearBackoff struct {
	min    time.Duration
	max    time.Duration
	step   time.Duration
	repeat int

	current     time.Duration
	repeatCount int
}

// NewLinearBackoff initialize new LinearBackoff strategy. All durations (min,
// max and step) should be positive and max needs to be greater than min.
// Otherwise non-nil error will be returned. Parameter repeat needs to be at
// least 1.
func NewLinearBackoff(min, max, step time.Duration, repeat int) (*LinearBackoff, error) {
	if min < 0 || max < 0 || step < 0 {
		return nil, errors.New("min, max, step durations should be positive")
	}
	if max <= min {
		return nil, fmt.Errorf("max should be greater than min (min:%v, max:%v)",
			min, max)
	}
	if repeat < 1 {
		return nil, errors.New("repeat needs to be at least 1")
	}

	return &LinearBackoff{
		min:     min,
		max:     max,
		step:    step,
		repeat:  repeat,
		current: min,
	}, nil
}

// NextInterval returns time duration, to pass before the next event.
func (lb *LinearBackoff) NextInterval() time.Duration {
	if lb.repeatCount < lb.repeat {
		lb.repeatCount++
	} else {
		lb.repeatCount = 1
		lb.current += lb.step
		if lb.current > lb.max {
			lb.current = lb.max
		}
	}
	return lb.current
}

// Reset resets internal LinearBackoff state. In particular next call to
// NextInterval should return min duration interval.
func (lb *LinearBackoff) Reset() {
	lb.current = lb.min
	lb.repeatCount = 0
}
