package dag

import (
	"testing"
	"time"
)

func TestFixedScheduleSimple(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	ts := timeForFixDay(12, 0, 0)
	fs := FixedSchedule{Start: start, Interval: 10 * time.Minute}
	next1 := fs.Next(ts)
	expNext1 := timeForFixDay(12, 10, 0)
	if next1 != expNext1 {
		t.Errorf("Expected next timestamp %v, got %v", expNext1, next1)
	}

	next2 := fs.Next(next1)
	expNext2 := timeForFixDay(12, 20, 0)
	if next2 != expNext2 {
		t.Errorf("Expected second next timestamp %v, got %v", expNext2, next2)
	}

	next3 := fs.Next(next2)
	expNext3 := timeForFixDay(12, 30, 0)
	if next3 != expNext3 {
		t.Errorf("Expected second next timestamp %v, got %v", expNext3, next3)
	}
}

func TestFixedScheduleBeforeStart(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	beforeStart := timeForFixDay(2, 0, 0)
	fs := FixedSchedule{Start: start, Interval: 10 * time.Minute}
	next := fs.Next(beforeStart)
	if next != start {
		t.Errorf("Next timestamp for input before Start should return start, got: %v", next)
	}
}

func TestFixedScheduleManyIterations(t *testing.T) {
	start := timeForFixDay(8, 0, 0)
	fs := FixedSchedule{Start: start, Interval: 1 * time.Millisecond}
	curr := start

	for i := 0; i < 1000; i++ {
		curr = fs.Next(curr)
	}

	exp := timeForFixDay(8, 0, 1) // 1s = 1000ms
	if curr != exp {
		t.Errorf("Expected %v for 1000 Next shedule ticks, got: %v", exp, curr)
	}
}

func BenchmarkFixedScheduleShort(b *testing.B) {
	fs := FixedSchedule{
		Start:    time.Date(2021, time.January, 12, 10, 0, 0, 0, time.UTC),
		Interval: 10 * time.Minute,
	}
	for i := 0; i < b.N; i++ {
		fs.Next(time.Now())
	}
}

func BenchmarkFixedScheduleLong(b *testing.B) {
	fs := FixedSchedule{
		Start:    time.Date(1970, time.January, 12, 10, 0, 0, 0, time.UTC),
		Interval: 10 * time.Minute,
	}
	for i := 0; i < b.N; i++ {
		fs.Next(time.Now())
	}
}

func timeForFixDay(hour, minute, second int) time.Time {
	return time.Date(2023, time.September, 24, hour, minute, second, 0, time.UTC)
}
