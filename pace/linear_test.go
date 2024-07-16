// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package pace

import (
	"testing"
	"time"
)

const (
	u  = 1 * time.Microsecond
	ms = 1 * time.Millisecond
	s  = 1 * time.Second
)

func TestNewLinearBackoff(t *testing.T) {
	data := []struct {
		min           time.Duration
		max           time.Duration
		step          time.Duration
		repeat        int
		expectedError bool
	}{
		{10 * ms, 100 * ms, 10 * ms, 1, false},
		{10 * ms, 100 * ms, 10 * ms, 0, true},
		{10 * ms, 100 * ms, 10 * ms, -123, true},
		{0 * ms, 100 * ms, 1 * time.Microsecond, 1, false},
		{10 * ms, 100 * ms, 10 * ms, 10, false},

		{100 * s, 10 * s, s, 1, true},
		{1 * s, -10 * s, -s, 10, true},
	}

	for _, d := range data {
		_, err := NewLinearBackoff(d.min, d.max, d.step, d.repeat)
		if err != nil && !d.expectedError {
			t.Errorf(
				"Not expected error for NewLinearBackoff(%v, %v, %v, %d), got: %v",
				d.min, d.max, d.step, d.repeat, err,
			)
		}
		if err == nil && d.expectedError {
			t.Errorf(
				"Expected non-nil error for NewLinearBackoff(%v, %v, %v, %d), got nil",
				d.min, d.max, d.step, d.repeat,
			)
		}
	}
}

func TestLinearBackoffRepeat1(t *testing.T) {
	min := 1 * ms
	max := 100 * ms
	step := 5 * ms

	lb, err := NewLinearBackoff(min, max, step, 1)
	if err != nil {
		t.Errorf("Failed while initializing LinearBackoff: %s", err.Error())
	}

	first := lb.NextInterval()
	second := lb.NextInterval()
	for i := 0; i < 12; i++ {
		lb.NextInterval()
	}
	fifteenth := lb.NextInterval()

	for i := 0; i < 1000; i++ {
		lb.NextInterval()
	}
	overThousand := lb.NextInterval()

	// asserts
	if first != min {
		t.Errorf("Expected the first interval to be %v, got: %v", min, first)
	}

	expSecond := 6 * ms
	if second != expSecond {
		t.Errorf("Expected the second interval to be %v, got: %v", expSecond,
			second)
	}

	expFifteenth := 71 * ms
	if fifteenth != expFifteenth {
		t.Errorf("Expected 15th interval to be %v, got: %v", expFifteenth,
			fifteenth)
	}

	if overThousand != max {
		t.Errorf("Expected >1000th interval, to be max (%v), got: %v", max,
			overThousand)
	}

	// Reset and assert
	lb.Reset()
	if next := lb.NextInterval(); next != min {
		t.Errorf("Expected the next interval after reset, to be %v, got: %v",
			min, next)
	}
}

func TestLinearBackoffRepeat1Monotonicity(t *testing.T) {
	const repeat = 1
	data := []struct {
		min        time.Duration
		max        time.Duration
		step       time.Duration
		iterations int
	}{
		{0, 100 * ms, 1 * ms, 2},
		{0, 100 * ms, 1 * ms, 100},
		{0, 100 * ms, 1 * ms, 200},
		{1 * u, 1 * ms, 17 * u, 5},
		{1 * u, 1 * ms, 17 * u, 100},
		{1 * u, 1 * ms, 17 * u, 650},
		{1 * s, 2 * s, 650 * ms, 2},
		{1 * s, 2 * s, 650 * ms, 5},
		{1 * s, 2 * s, 10 * s, 2},
		{1 * s, 2 * s, 10 * s, 5},
	}

	for _, d := range data {
		lb, newErr := NewLinearBackoff(d.min, d.max, d.step, repeat)
		if newErr != nil {
			t.Errorf("Error while initializing LinearBackoff(%v, %v, %v, %d): %s",
				d.min, d.max, d.step, repeat, newErr.Error())
			continue
		}
		prev := lb.NextInterval()
		for i := 0; i < d.iterations-1; i++ {
			curr := lb.NextInterval()
			isMonotonic := curr == d.max || prev < curr
			if !isMonotonic {
				t.Errorf("Found non-monotonic intervals: id=%d prev=%v curr=%v",
					i, prev, curr)
			}
			prev = curr
		}
	}
}

func TestLinearBackoffFlatoutAtMax(t *testing.T) {
	data := []struct {
		min         time.Duration
		max         time.Duration
		step        time.Duration
		repeat      int
		intervalNum int
		expectedMax bool
	}{
		// repeat = 1
		{0, 100 * ms, 1 * ms, 1, 100, false},
		{0, 100 * ms, 1 * ms, 1, 101, true},
		{0, 100 * ms, 1 * ms, 1, 2500, true},
		{10 * u, 1 * s, 1 * ms, 1, 990, false},
		{10 * u, 1 * s, 1 * ms, 1, 1001, true},
		{10 * u, 1 * s, 1 * ms, 1, 5000, true},
		{1 * s, 1500 * ms, 2 * s, 1, 1, false},
		{1 * s, 1500 * ms, 2 * s, 1, 2, true},

		// repeat > 1
		{0, 100 * ms, 1 * ms, 3, 100, false},
		{0, 100 * ms, 1 * ms, 3, 300, false},
		{0, 100 * ms, 1 * ms, 3, 301, true},
		{0, 100 * ms, 1 * ms, 3, 1000, true},
		{1 * s, 1500 * ms, 2 * s, 3, 1, false},
		{1 * s, 1500 * ms, 2 * s, 3, 3, false},
		{1 * s, 1500 * ms, 2 * s, 3, 4, true},
		{1 * s, 1500 * ms, 2 * s, 3, 6, true},
		{1 * s, 1500 * ms, 2 * s, 3, 1000, true},

		// repeat = 1000
		{0, 100 * ms, 1 * ms, 1000, 1, false},
		{0, 100 * ms, 1 * ms, 1000, 1000, false},
		{0, 100 * ms, 1 * ms, 1000, 100 * 1000, false},
		{0, 100 * ms, 1 * ms, 1000, 100*1000 + 1, true},
	}

	for _, d := range data {
		lb, newErr := NewLinearBackoff(d.min, d.max, d.step, d.repeat)
		if newErr != nil {
			t.Errorf("Error while initializing LinearBackoff(%v, %v, %v, %d): %s",
				d.min, d.max, d.step, d.repeat, newErr.Error())
			continue
		}
		for i := 0; i < d.intervalNum-1; i++ {
			lb.NextInterval()
		}
		targetInterval := lb.NextInterval()

		if d.expectedMax && targetInterval != d.max {
			t.Errorf("Expected %dth interval to be max=%v, got: %v",
				d.intervalNum, d.max, targetInterval)
		}
		if !d.expectedMax && targetInterval == d.max {
			t.Errorf("Expected %dth interval to not reach max=%v, got: %v",
				d.intervalNum, d.max, targetInterval)
		}
	}
}

func TestLinearBackoffResets(t *testing.T) {
	data := []struct {
		min                    time.Duration
		max                    time.Duration
		step                   time.Duration
		repeat                 int
		intervalNumBeforeReset int
	}{
		{0, 100 * ms, 1 * ms, 1, 1},
		{0, 100 * ms, 1 * ms, 1, 100},
		{0, 100 * ms, 1 * ms, 100, 100},
		{99 * u, 1 * ms, 1 * u, 1, 1},
		{99 * u, 1 * ms, 1 * u, 1, 2},
		{99 * u, 1 * ms, 1 * u, 1, 14},
		{99 * u, 1 * ms, 1 * u, 1, 100},
		{99 * u, 1 * ms, 1 * u, 13, 1},
		{99 * u, 1 * ms, 1 * u, 13, 2},
		{99 * u, 1 * ms, 1 * u, 13, 14},
		{99 * u, 1 * ms, 1 * u, 13, 100},
	}

	for _, d := range data {
		lb, newErr := NewLinearBackoff(d.min, d.max, d.step, d.repeat)
		if newErr != nil {
			t.Errorf("Error while initializing LinearBackoff(%v, %v, %v, %d): %s",
				d.min, d.max, d.step, d.repeat, newErr.Error())
			continue
		}
		for i := 0; i < d.intervalNumBeforeReset; i++ {
			lb.NextInterval()
		}
		lb.Reset()
		if next := lb.NextInterval(); next != d.min {
			t.Errorf("Expected next interval after Reset, to be min=%v, got: %v",
				d.min, next)
		}
	}
}
