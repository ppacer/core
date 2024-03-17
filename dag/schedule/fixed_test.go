// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package schedule

import (
	"testing"
	"time"
)

func TestNewFixed(t *testing.T) {
	now := time.Now()
	interval := 1 * time.Millisecond
	fs := NewFixed(now, interval)

	if !fs.start.Equal(now) {
		t.Errorf("Expected schedule start to be %+v, but got: %+v",
			now, fs.start)
	}
	if fs.interval != interval {
		t.Errorf("Expected schedule start to be %+v, but got: %+v",
			interval, fs.interval)
	}
}

func TestFixedSimple(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	ts := timeForFixDay(12, 0, 0)
	fs := NewFixed(start, 10*time.Minute)
	next1 := fs.Next(ts, nil)
	expNext1 := timeForFixDay(12, 10, 0)
	if next1 != expNext1 {
		t.Errorf("Expected next timestamp %v, got %v", expNext1, next1)
	}

	next2 := fs.Next(next1, nil)
	expNext2 := timeForFixDay(12, 20, 0)
	if next2 != expNext2 {
		t.Errorf("Expected second next timestamp %v, got %v", expNext2, next2)
	}

	next3 := fs.Next(next2, nil)
	expNext3 := timeForFixDay(12, 30, 0)
	if next3 != expNext3 {
		t.Errorf("Expected second next timestamp %v, got %v", expNext3, next3)
	}
}

func TestFixedWithPrevSched(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	fs := NewFixed(start, 10*time.Minute)

	data := []struct {
		prevSched    time.Time
		currentTime  time.Time
		expectedNext time.Time
	}{
		{
			timeForFixDay(11, 20, 0),
			timeForFixDay(11, 20, 0),
			timeForFixDay(11, 30, 0),
		},
		{
			timeForFixDay(11, 20, 0),
			timeForFixDay(11, 28, 0),
			timeForFixDay(11, 30, 0),
		},
		{
			timeForFixDay(11, 20, 0),
			timeForFixDay(11, 30, 0),
			timeForFixDay(11, 30, 0),
		},
		{
			timeForFixDay(11, 20, 0),
			timeForFixDay(11, 35, 0),
			timeForFixDay(11, 30, 0),
		},
	}

	for _, d := range data {
		next := fs.Next(d.currentTime, &d.prevSched)
		if !d.expectedNext.Equal(next) {
			t.Errorf("Expected next schedule to be at %v, got %v",
				d.expectedNext, next)
		}
	}
}

func TestFixedBeforeStart(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	beforeStart := timeForFixDay(2, 0, 0)
	fs := NewFixed(start, 10*time.Minute)
	next := fs.Next(beforeStart, nil)
	if next != start {
		t.Errorf("Next timestamp for input before Start should return start, got: %v",
			next)
	}
}

func TestFixedManyIterations(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	fs := NewFixed(start, 1*time.Millisecond)
	curr := start

	for i := 0; i < 1000; i++ {
		curr = fs.Next(curr, nil)
	}

	exp := timeForFixDay(8, 0, 1) // 1s = 1000ms
	if curr != exp {
		t.Errorf("Expected %v for 1000 Next shedule ticks, got: %v", exp, curr)
	}
}

func BenchmarkFixedShort(b *testing.B) {
	start := time.Date(2021, time.January, 12, 10, 0, 0, 0, time.UTC)
	fs := NewFixed(start, 10*time.Minute)
	for i := 0; i < b.N; i++ {
		fs.Next(time.Now(), nil)
	}
}

func BenchmarkFixedLong(b *testing.B) {
	start := time.Date(1970, time.January, 12, 10, 0, 0, 0, time.UTC)
	fs := NewFixed(start, 10*time.Minute)
	for i := 0; i < b.N; i++ {
		fs.Next(time.Now(), nil)
	}
}

func BenchmarkFixedLongWithPrevSched(b *testing.B) {
	start := time.Date(1970, time.January, 12, 10, 0, 0, 0, time.UTC)
	fs := NewFixed(start, 10*time.Minute)
	prevSched := time.Date(2024, time.February, 14, 14, 20, 0, 0, time.UTC)
	for i := 0; i < b.N; i++ {
		fs.Next(time.Now(), &prevSched)
	}
}

func timeForFixDay(hour, minute, second int) time.Time {
	return time.Date(2023, time.September, 24, hour, minute, second, 0, time.UTC)
}
