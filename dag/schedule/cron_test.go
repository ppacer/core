package schedule

import (
	"testing"
	"time"
)

func TestDefaultCron(t *testing.T) {
	c := NewCron()
	if !c.isDefault() {
		t.Errorf("Expected default cron * * * * * from NewCron()")
	}
}

func TestCronString(t *testing.T) {
	data := []struct {
		input    *Cron
		expected string
	}{
		{NewCron(), "* * * * *"},
		{NewCron().AtMinute(10), "10 * * * *"},
		{NewCron().AtMinutes(5, 30, 55), "5,30,55 * * * *"},
		{NewCron().AtHour(12).AtMinute(59), "59 12 * * *"},
		{NewCron().AtHour(12).OnWeekday(time.Monday).AtMinute(59), "59 12 * * 1"},
		{NewCron().OnWeekday(time.Monday).AtMinute(59).AtHour(12), "59 12 * * 1"},
	}

	for _, d := range data {
		res := d.input.String()
		if res != d.expected {
			t.Errorf("Expected %s, but got: %s for input %v", d.expected, res,
				d.input)
		}
	}
}

// * * * * *
func TestCronNextDefualt(t *testing.T) {
	data := []struct {
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{timeUtc(2024, 3, 24, 12, 10), timeUtc(2024, 3, 24, 12, 11)},
		{timeUtc(2024, 3, 23, 23, 59), timeUtc(2024, 3, 24, 0, 0)},
		// TODO: more cases
	}

	cronSched := NewCron()
	for _, d := range data {
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// n * * * *
func TestCronMinuteNext(t *testing.T) {
	warsawTz := warsaw(t)
	beforeDlsWarsaw := time.Date(2024, time.March, 31, 1, 59, 0, 0, warsawTz)

	data := []struct {
		cronMinute       int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{0, timeUtc(2024, 3, 24, 12, 0), timeUtc(2024, 3, 24, 13, 0)},
		{0, timeUtc(2024, 3, 24, 12, 10), timeUtc(2024, 3, 24, 13, 0)},
		{0, timeUtc(2024, 12, 31, 23, 1), timeUtc(2025, 1, 1, 0, 0)},
		{10, timeUtc(2024, 3, 23, 23, 59), timeUtc(2024, 3, 24, 0, 10)},
		{13, timeUtc(2024, 3, 31, 23, 59), timeUtc(2024, 4, 1, 0, 13)},
		{59, timeUtc(2024, 12, 31, 23, 50), timeUtc(2024, 12, 31, 23, 59)},
		{59, timeUtc(2024, 12, 31, 23, 59), timeUtc(2025, 1, 1, 0, 59)},

		// daylight saving change
		{5, beforeDlsWarsaw, time.Date(2024, time.March, 31, 3, 5, 0, 0, warsawTz)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinute(d.cronMinute)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// n,m * * * *
func TestCronMinutesNext(t *testing.T) {
	data := []struct {
		cronMinutes      []int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{[]int{0, 5}, timeUtc(2024, 3, 24, 12, 10), timeUtc(2024, 3, 24, 13, 0)},
		{[]int{0, 5}, timeUtc(2024, 3, 24, 12, 3), timeUtc(2024, 3, 24, 12, 5)},
		{[]int{5, 0}, timeUtc(2024, 3, 24, 12, 3), timeUtc(2024, 3, 24, 12, 5)},
		{[]int{15, 16, 59}, timeUtc(2024, 3, 24, 12, 3), timeUtc(2024, 3, 24, 12, 15)},
		{[]int{15, 16, 59}, timeUtc(2024, 3, 24, 12, 15), timeUtc(2024, 3, 24, 12, 16)},
		{[]int{15, 16, 59}, timeUtc(2024, 3, 24, 12, 16), timeUtc(2024, 3, 24, 12, 59)},
		{[]int{15, 16, 59}, timeUtc(2024, 3, 24, 12, 30), timeUtc(2024, 3, 24, 12, 59)},
		{[]int{15, 16, 59}, timeUtc(2024, 3, 24, 12, 59), timeUtc(2024, 3, 24, 13, 15)},
		{[]int{22, 44}, timeUtc(2024, 3, 31, 23, 40), timeUtc(2024, 3, 31, 23, 44)},
		{[]int{22, 44}, timeUtc(2024, 3, 31, 23, 55), timeUtc(2024, 4, 1, 0, 22)},
		{[]int{22, 44}, timeUtc(2024, 12, 31, 23, 40), timeUtc(2024, 12, 31, 23, 44)},
		{[]int{22, 44}, timeUtc(2024, 12, 31, 23, 58), timeUtc(2025, 1, 1, 0, 22)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinutes(d.cronMinutes...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * h * * *
func TestCronEveryMinuteHourNext(t *testing.T) {
	warsawTz := warsaw(t)
	beforeDlsWarsaw := time.Date(2024, time.March, 31, 1, 59, 0, 0, warsawTz)

	data := []struct {
		cronHour         int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{12, timeUtc(2024, 3, 24, 11, 0), timeUtc(2024, 3, 24, 12, 0)},
		{12, timeUtc(2024, 3, 24, 12, 5), timeUtc(2024, 3, 24, 12, 6)},
		{12, timeUtc(2024, 3, 24, 12, 59), timeUtc(2024, 3, 25, 12, 0)},
		{5, timeUtc(2024, 3, 31, 4, 45), timeUtc(2024, 3, 31, 5, 0)},
		{5, timeUtc(2024, 3, 31, 5, 0), timeUtc(2024, 3, 31, 5, 1)},
		{5, timeUtc(2024, 3, 31, 5, 16), timeUtc(2024, 3, 31, 5, 17)},
		{5, timeUtc(2024, 3, 31, 5, 59), timeUtc(2024, 4, 1, 5, 0)},
		{5, timeUtc(2024, 3, 31, 13, 0), timeUtc(2024, 4, 1, 5, 0)},
		{23, timeUtc(2024, 12, 31, 21, 45), timeUtc(2024, 12, 31, 23, 0)},
		{23, timeUtc(2024, 12, 31, 23, 0), timeUtc(2024, 12, 31, 23, 1)},
		{23, timeUtc(2024, 12, 31, 23, 16), timeUtc(2024, 12, 31, 23, 17)},
		{23, timeUtc(2024, 12, 31, 23, 59), timeUtc(2025, 1, 1, 23, 0)},
		{23, timeUtc(2025, 1, 1, 0, 0), timeUtc(2025, 1, 1, 23, 0)},

		{2, beforeDlsWarsaw, time.Date(2024, time.April, 1, 2, 0, 0, 0, warsawTz)},
	}

	for _, d := range data {
		cronSched := NewCron().AtHour(d.cronHour)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * h,g * * *
func TestCronEveryMinuteHoursNext(t *testing.T) {
	data := []struct {
		cronHours        []int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{[]int{12, 20}, timeUtc(2024, 3, 24, 11, 0), timeUtc(2024, 3, 24, 12, 0)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 12, 0), timeUtc(2024, 3, 24, 12, 1)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 12, 30), timeUtc(2024, 3, 24, 12, 31)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 12, 59), timeUtc(2024, 3, 24, 20, 0)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 19, 15), timeUtc(2024, 3, 24, 20, 0)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 20, 0), timeUtc(2024, 3, 24, 20, 1)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 20, 30), timeUtc(2024, 3, 24, 20, 31)},
		{[]int{12, 20}, timeUtc(2024, 3, 24, 20, 59), timeUtc(2024, 3, 25, 12, 0)},

		{[]int{0, 23}, timeUtc(2024, 3, 31, 19, 5), timeUtc(2024, 3, 31, 23, 0)},
		{[]int{0, 23}, timeUtc(2024, 3, 31, 23, 0), timeUtc(2024, 3, 31, 23, 1)},
		{[]int{0, 23}, timeUtc(2024, 3, 31, 23, 30), timeUtc(2024, 3, 31, 23, 31)},
		{[]int{0, 23}, timeUtc(2024, 3, 31, 23, 59), timeUtc(2024, 4, 1, 0, 0)},
		{[]int{0, 23}, timeUtc(2024, 4, 1, 0, 15), timeUtc(2024, 4, 1, 0, 16)},
		{[]int{0, 23}, timeUtc(2024, 4, 1, 0, 59), timeUtc(2024, 4, 1, 23, 0)},

		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 6, 15), timeUtc(2024, 12, 31, 8, 0)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 8, 0), timeUtc(2024, 12, 31, 8, 1)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 10, 0), timeUtc(2024, 12, 31, 17, 0)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 17, 49), timeUtc(2024, 12, 31, 17, 50)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 17, 59), timeUtc(2024, 12, 31, 22, 0)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 22, 0), timeUtc(2024, 12, 31, 22, 1)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 22, 19), timeUtc(2024, 12, 31, 22, 20)},
		{[]int{8, 17, 22}, timeUtc(2024, 12, 31, 22, 59), timeUtc(2025, 1, 1, 8, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().AtHours(d.cronHours...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m h * * *
func TestCronMinuteHourNext(t *testing.T) {
	data := []struct {
		cronMinute       int
		cronHour         int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{0, 12, timeUtc(2024, 3, 24, 11, 0), timeUtc(2024, 3, 24, 12, 0)},
		{0, 12, timeUtc(2024, 3, 24, 12, 0), timeUtc(2024, 3, 25, 12, 0)},
		{0, 12, timeUtc(2024, 3, 24, 12, 1), timeUtc(2024, 3, 25, 12, 0)},
		{15, 12, timeUtc(2024, 3, 24, 11, 0), timeUtc(2024, 3, 24, 12, 15)},
		{15, 12, timeUtc(2024, 3, 24, 12, 0), timeUtc(2024, 3, 24, 12, 15)},
		{15, 12, timeUtc(2024, 3, 24, 12, 1), timeUtc(2024, 3, 24, 12, 15)},
		{13, 0, timeUtc(2024, 3, 24, 0, 0), timeUtc(2024, 3, 24, 0, 13)},
		{13, 0, timeUtc(2024, 3, 24, 12, 0), timeUtc(2024, 3, 25, 0, 13)},
		{13, 0, timeUtc(2024, 3, 24, 0, 13), timeUtc(2024, 3, 25, 0, 13)},
		{0, 0, timeUtc(2024, 3, 24, 0, 13), timeUtc(2024, 3, 25, 0, 0)},
		{0, 0, timeUtc(2024, 3, 24, 0, 0), timeUtc(2024, 3, 25, 0, 0)},
		{0, 0, timeUtc(2024, 3, 24, 23, 59), timeUtc(2024, 3, 25, 0, 0)},
		{0, 0, timeUtc(2024, 12, 31, 1, 1), timeUtc(2025, 1, 1, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinute(d.cronMinute).AtHour(d.cronHour)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m,n h * * *
func TestCronMinutesHourNext(t *testing.T) {
	data := []struct {
		cronMinute       []int
		cronHour         int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{[]int{2, 18, 48}, 12, timeUtc(2024, 3, 24, 11, 15), timeUtc(2024, 3, 24, 12, 2)},
		{[]int{2, 18, 48}, 12, timeUtc(2024, 3, 24, 11, 0), timeUtc(2024, 3, 24, 12, 2)},
		{[]int{2, 18, 48}, 12, timeUtc(2024, 3, 24, 12, 2), timeUtc(2024, 3, 24, 12, 18)},
		{[]int{2, 18, 48}, 12, timeUtc(2024, 3, 24, 12, 30), timeUtc(2024, 3, 24, 12, 48)},
		{[]int{2, 18, 48}, 12, timeUtc(2024, 3, 24, 12, 49), timeUtc(2024, 3, 25, 12, 2)},

		{[]int{0, 59}, 23, timeUtc(2024, 3, 31, 22, 0), timeUtc(2024, 3, 31, 23, 0)},
		{[]int{0, 59}, 23, timeUtc(2024, 3, 31, 23, 0), timeUtc(2024, 3, 31, 23, 59)},
		{[]int{0, 59}, 23, timeUtc(2024, 3, 31, 23, 30), timeUtc(2024, 3, 31, 23, 59)},
		{[]int{0, 59}, 23, timeUtc(2024, 3, 31, 23, 59), timeUtc(2024, 4, 1, 23, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinutes(d.cronMinute...).AtHour(d.cronHour)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m h,g * * *
func TestCronMinuteHoursNext(t *testing.T) {
	data := []struct {
		cronMinute       int
		cronHours        []int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 11, 15), timeUtc(2024, 3, 24, 13, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 12, 55), timeUtc(2024, 3, 24, 13, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 13, 53), timeUtc(2024, 3, 24, 13, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 13, 55), timeUtc(2024, 3, 24, 23, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 19, 0), timeUtc(2024, 3, 24, 23, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 23, 45), timeUtc(2024, 3, 24, 23, 55)},
		{55, []int{13, 23}, timeUtc(2024, 3, 24, 23, 55), timeUtc(2024, 3, 25, 13, 55)},

		{0, []int{1}, timeUtc(2024, 12, 31, 0, 30), timeUtc(2024, 12, 31, 1, 0)},
		{0, []int{1}, timeUtc(2024, 12, 31, 1, 0), timeUtc(2025, 1, 1, 1, 0)},
		{0, []int{1}, timeUtc(2024, 12, 31, 18, 33), timeUtc(2025, 1, 1, 1, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinute(d.cronMinute).AtHours(d.cronHours...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m,n h,g * * *
func TestCronMinutesHoursNext(t *testing.T) {
	data := []struct {
		cronMinutes      []int
		cronHours        []int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 0, 0), timeUtc(2024, 3, 24, 0, 3)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 0, 3), timeUtc(2024, 3, 24, 0, 4)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 0, 4), timeUtc(2024, 3, 24, 12, 3)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 0, 5), timeUtc(2024, 3, 24, 12, 3)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 11, 35), timeUtc(2024, 3, 24, 12, 3)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 12, 3), timeUtc(2024, 3, 24, 12, 4)},
		{[]int{3, 4}, []int{0, 12}, timeUtc(2024, 3, 24, 12, 4), timeUtc(2024, 3, 25, 0, 3)},
	}

	for _, d := range data {
		cronSched := NewCron().AtMinutes(d.cronMinutes...).AtHours(d.cronHours...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d * *
func TestCronDomNext(t *testing.T) {
	data := []struct {
		dayOfMonth       int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{13, timeUtc(2024, 3, 10, 12, 0), timeUtc(2024, 3, 13, 0, 0)},
		{13, timeUtc(2024, 3, 13, 12, 0), timeUtc(2024, 3, 13, 12, 1)},
		{13, timeUtc(2024, 3, 13, 12, 59), timeUtc(2024, 3, 13, 13, 0)},
		{13, timeUtc(2024, 3, 13, 23, 59), timeUtc(2024, 4, 13, 0, 0)},
		{3, timeUtc(2024, 3, 13, 12, 0), timeUtc(2024, 4, 3, 0, 0)},
		{3, timeUtc(2024, 12, 5, 12, 0), timeUtc(2025, 1, 3, 0, 0)},
		{1, timeUtc(2024, 12, 1, 23, 59), timeUtc(2025, 1, 1, 0, 0)},
		{1, timeUtc(2024, 12, 2, 0, 0), timeUtc(2025, 1, 1, 0, 0)},
		{31, timeUtc(2024, 2, 27, 12, 0), timeUtc(2024, 3, 31, 0, 0)},
		{31, timeUtc(2024, 4, 1, 12, 0), timeUtc(2024, 5, 31, 0, 0)},
		{31, timeUtc(2024, 1, 31, 23, 59), timeUtc(2024, 3, 31, 0, 0)},
		{10, timeUtc(2024, 12, 1, 23, 59), timeUtc(2024, 12, 10, 0, 0)},
		{10, timeUtc(2024, 12, 10, 1, 1), timeUtc(2024, 12, 10, 1, 2)},
		{10, timeUtc(2024, 12, 10, 23, 59), timeUtc(2025, 1, 10, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().OnMonthDay(d.dayOfMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d,e * *
func TestCronDomsNext(t *testing.T) {
	data := []struct {
		dayOfMonth       []int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{[]int{13, 30}, timeUtc(2024, 3, 10, 12, 0), timeUtc(2024, 3, 13, 0, 0)},
		{[]int{13, 30}, timeUtc(2024, 3, 13, 12, 0), timeUtc(2024, 3, 13, 12, 1)},
		{[]int{13, 30}, timeUtc(2024, 3, 13, 23, 59), timeUtc(2024, 3, 30, 0, 0)},
		{[]int{13, 30}, timeUtc(2024, 3, 30, 12, 0), timeUtc(2024, 3, 30, 12, 1)},
		{[]int{13, 30}, timeUtc(2024, 3, 30, 23, 59), timeUtc(2024, 4, 13, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().OnMonthDays(d.dayOfMonth...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * h d * *
func TestCronHourDomNext(t *testing.T) {
	data := []struct {
		hour             int
		dayOfMonth       int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{5, 13, timeUtc(2024, 3, 10, 22, 13), timeUtc(2024, 3, 13, 5, 0)},
		{5, 13, timeUtc(2024, 3, 13, 2, 10), timeUtc(2024, 3, 13, 5, 0)},
		{5, 13, timeUtc(2024, 3, 13, 5, 0), timeUtc(2024, 3, 13, 5, 1)},
		{5, 13, timeUtc(2024, 3, 13, 5, 59), timeUtc(2024, 4, 13, 5, 0)},
		{23, 31, timeUtc(2024, 12, 13, 5, 59), timeUtc(2024, 12, 31, 23, 0)},
		{23, 31, timeUtc(2024, 12, 31, 23, 3), timeUtc(2024, 12, 31, 23, 4)},
		{23, 31, timeUtc(2024, 12, 31, 23, 59), timeUtc(2025, 1, 31, 23, 0)},
		{23, 31, timeUtc(2025, 1, 31, 23, 16), timeUtc(2025, 1, 31, 23, 17)},
		{23, 31, timeUtc(2025, 1, 31, 23, 59), timeUtc(2025, 3, 31, 23, 0)},
		{23, 31, timeUtc(2025, 3, 31, 23, 0), timeUtc(2025, 3, 31, 23, 1)},
	}

	for _, d := range data {
		cronSched := NewCron().AtHour(d.hour).OnMonthDay(d.dayOfMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m h d * *
func TestCronMinuteHourDomNext(t *testing.T) {
	warsawTz := warsaw(t)

	data := []struct {
		minute           int
		hour             int
		dayOfMonth       int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{0, 11, 30, timeUtc(2024, 3, 10, 22, 13), timeUtc(2024, 3, 30, 11, 0)},
		{0, 11, 30, timeUtc(2024, 3, 30, 11, 1), timeUtc(2024, 4, 30, 11, 0)},
		{0, 11, 30, timeUtc(2024, 3, 30, 11, 0), timeUtc(2024, 4, 30, 11, 0)},
		{1, 2, 3, timeUtc(2024, 12, 1, 0, 0), timeUtc(2024, 12, 3, 2, 1)},
		{1, 2, 3, timeUtc(2024, 12, 3, 0, 0), timeUtc(2024, 12, 3, 2, 1)},
		{1, 2, 3, timeUtc(2024, 12, 3, 2, 0), timeUtc(2024, 12, 3, 2, 1)},
		{1, 2, 3, timeUtc(2024, 12, 3, 2, 1), timeUtc(2025, 1, 3, 2, 1)},

		{0, 2, 31, time.Date(2024, time.March, 1, 12, 0, 0, 0, warsawTz),
			time.Date(2024, time.May, 31, 2, 0, 0, 0, warsawTz)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtHour(d.hour).
			AtMinute(d.minute).
			OnMonthDay(d.dayOfMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m,n h,g d * *
func TestCronMinutesHoursDomNext(t *testing.T) {
	data := []struct {
		minute           []int
		hour             []int
		dayOfMonth       int
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 10, 22, 13), timeUtc(2024, 3, 29, 11, 5),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 1, 0), timeUtc(2024, 3, 29, 11, 5),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 11, 5), timeUtc(2024, 3, 29, 11, 15),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 11, 10), timeUtc(2024, 3, 29, 11, 15),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 11, 15), timeUtc(2024, 3, 29, 23, 5),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 14, 0), timeUtc(2024, 3, 29, 23, 5),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 23, 5), timeUtc(2024, 3, 29, 23, 15),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 23, 12), timeUtc(2024, 3, 29, 23, 15),
		},
		{
			[]int{5, 15}, []int{11, 23}, 29,
			timeUtc(2024, 3, 29, 23, 15), timeUtc(2024, 4, 29, 11, 5),
		},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtHours(d.hour...).
			AtMinutes(d.minute...).
			OnMonthDay(d.dayOfMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * * m *
func TestCronMonthNext(t *testing.T) {
	data := []struct {
		cronMonth        time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{time.February, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 2, 1, 0, 0)},
		{time.February, timeUtc(2024, 1, 31, 23, 0), timeUtc(2024, 2, 1, 0, 0)},
		{time.February, timeUtc(2024, 1, 31, 23, 59), timeUtc(2024, 2, 1, 0, 0)},
		{time.February, timeUtc(2024, 2, 1, 0, 0), timeUtc(2024, 2, 1, 0, 1)},
		{time.February, timeUtc(2024, 2, 1, 1, 0), timeUtc(2024, 2, 1, 1, 1)},
		{time.February, timeUtc(2024, 2, 1, 23, 59), timeUtc(2024, 2, 2, 0, 0)},
		{time.February, timeUtc(2024, 2, 29, 23, 59), timeUtc(2025, 2, 1, 0, 0)},
		{time.February, timeUtc(2023, 2, 28, 23, 59), timeUtc(2024, 2, 1, 0, 0)},
		{time.January, timeUtc(2024, 12, 01, 0, 0), timeUtc(2025, 1, 1, 0, 0)},
		{time.January, timeUtc(2024, 12, 24, 23, 0), timeUtc(2025, 1, 1, 0, 0)},
		{time.January, timeUtc(2024, 12, 31, 23, 59), timeUtc(2025, 1, 1, 0, 0)},
		{time.January, timeUtc(2025, 1, 1, 0, 0), timeUtc(2025, 1, 1, 0, 1)},
		{time.January, timeUtc(2025, 1, 1, 12, 15), timeUtc(2025, 1, 1, 12, 16)},
		{time.January, timeUtc(2025, 1, 31, 23, 59), timeUtc(2026, 1, 1, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().InMonth(d.cronMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * * m,n *
func TestCronMonthsNext(t *testing.T) {
	months := []time.Month{time.February, time.October}
	data := []struct {
		cronMonths       []time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{months, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 2, 1, 0, 0)},
		{months, timeUtc(2024, 2, 1, 0, 0), timeUtc(2024, 2, 1, 0, 1)},
		{months, timeUtc(2024, 2, 19, 12, 59), timeUtc(2024, 2, 19, 13, 0)},
		{months, timeUtc(2024, 2, 29, 23, 59), timeUtc(2024, 10, 1, 0, 0)},
		{months, timeUtc(2024, 6, 30, 10, 59), timeUtc(2024, 10, 1, 0, 0)},
		{months, timeUtc(2024, 10, 1, 0, 0), timeUtc(2024, 10, 1, 0, 1)},
		{months, timeUtc(2024, 10, 21, 21, 21), timeUtc(2024, 10, 21, 21, 22)},
		{months, timeUtc(2024, 10, 31, 23, 59), timeUtc(2025, 2, 1, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().InMonths(d.cronMonths...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m h * m *
func TestCronMinuteHourMonth(t *testing.T) {
	data := []struct {
		cronMinute       int
		cronHour         int
		cronMonth        time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{27, 13, time.February, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 2, 1, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 1, 31, 23, 0), timeUtc(2024, 2, 1, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 2, 1, 0, 0), timeUtc(2024, 2, 1, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 2, 1, 13, 27), timeUtc(2024, 2, 2, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 2, 15, 15, 27), timeUtc(2024, 2, 16, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 2, 29, 11, 0), timeUtc(2024, 2, 29, 13, 27)},
		{27, 13, time.February, timeUtc(2024, 2, 29, 23, 0), timeUtc(2025, 2, 1, 13, 27)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtMinute(d.cronMinute).
			AtHour(d.cronHour).
			InMonth(d.cronMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m h d m *
func TestCronMinuteHourDayMonth(t *testing.T) {
	data := []struct {
		cronMinute       int
		cronHour         int
		cronDay          int
		cronMonth        time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{4, 22, 8, time.February, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 2, 8, 22, 4)},
		{4, 22, 8, time.February, timeUtc(2024, 1, 31, 23, 0), timeUtc(2024, 2, 8, 22, 4)},
		{4, 22, 8, time.February, timeUtc(2024, 2, 1, 0, 0), timeUtc(2024, 2, 8, 22, 4)},
		{4, 22, 8, time.February, timeUtc(2024, 2, 8, 10, 0), timeUtc(2024, 2, 8, 22, 4)},
		{4, 22, 8, time.February, timeUtc(2024, 2, 8, 22, 4), timeUtc(2025, 2, 8, 22, 4)},
		{13, 13, 29, time.February, timeUtc(2024, 1, 1, 22, 4), timeUtc(2024, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2024, 2, 1, 0, 9), timeUtc(2024, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2024, 2, 29, 13, 0), timeUtc(2024, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2024, 2, 29, 13, 13), timeUtc(2028, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2024, 3, 1, 9, 0), timeUtc(2028, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2025, 2, 28, 13, 13), timeUtc(2028, 2, 29, 13, 13)},
		{13, 13, 29, time.February, timeUtc(2028, 2, 29, 13, 13), timeUtc(2032, 2, 29, 13, 13)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtMinute(d.cronMinute).
			AtHour(d.cronHour).
			OnMonthDay(d.cronDay).
			InMonth(d.cronMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d m *
func TestCronDayMonth(t *testing.T) {
	data := []struct {
		cronDay          int
		cronMonth        time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{7, time.September, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 9, 7, 0, 0)},
		{7, time.September, timeUtc(2024, 9, 1, 11, 0), timeUtc(2024, 9, 7, 0, 0)},
		{7, time.September, timeUtc(2024, 9, 7, 0, 0), timeUtc(2024, 9, 7, 0, 1)},
		{7, time.September, timeUtc(2024, 9, 7, 0, 59), timeUtc(2024, 9, 7, 1, 0)},
		{7, time.September, timeUtc(2024, 9, 7, 11, 59), timeUtc(2024, 9, 7, 12, 0)},
		{7, time.September, timeUtc(2024, 9, 7, 20, 14), timeUtc(2024, 9, 7, 20, 15)},
		{7, time.September, timeUtc(2024, 9, 7, 23, 59), timeUtc(2025, 9, 7, 0, 0)},
		{29, time.February, timeUtc(2024, 2, 1, 10, 11), timeUtc(2024, 2, 29, 0, 0)},
		{29, time.February, timeUtc(2024, 2, 28, 22, 11), timeUtc(2024, 2, 29, 0, 0)},
		{29, time.February, timeUtc(2024, 2, 29, 0, 11), timeUtc(2024, 2, 29, 0, 12)},
		{29, time.February, timeUtc(2024, 2, 29, 23, 11), timeUtc(2024, 2, 29, 23, 12)},
		{29, time.February, timeUtc(2024, 2, 29, 23, 59), timeUtc(2028, 2, 29, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().
			OnMonthDay(d.cronDay).
			InMonth(d.cronMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d m,n *
func TestCronDayMonths(t *testing.T) {
	months := []time.Month{time.February, time.November}
	data := []struct {
		cronDay          int
		cronMonths       []time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{29, months, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 2, 29, 0, 0)},
		{29, months, timeUtc(2024, 1, 31, 23, 0), timeUtc(2024, 2, 29, 0, 0)},
		{29, months, timeUtc(2024, 2, 15, 12, 13), timeUtc(2024, 2, 29, 0, 0)},
		{29, months, timeUtc(2024, 2, 28, 23, 59), timeUtc(2024, 2, 29, 0, 0)},
		{29, months, timeUtc(2024, 2, 29, 0, 0), timeUtc(2024, 2, 29, 0, 1)},
		{29, months, timeUtc(2024, 2, 29, 3, 37), timeUtc(2024, 2, 29, 3, 38)},
		{29, months, timeUtc(2024, 2, 29, 23, 59), timeUtc(2024, 11, 29, 0, 0)},
		{29, months, timeUtc(2024, 9, 19, 9, 59), timeUtc(2024, 11, 29, 0, 0)},
		{29, months, timeUtc(2024, 11, 19, 9, 59), timeUtc(2024, 11, 29, 0, 0)},
		{29, months, timeUtc(2024, 11, 28, 23, 30), timeUtc(2024, 11, 29, 0, 0)},
		{29, months, timeUtc(2024, 11, 29, 0, 0), timeUtc(2024, 11, 29, 0, 1)},
		{29, months, timeUtc(2024, 11, 29, 1, 1), timeUtc(2024, 11, 29, 1, 2)},
		{29, months, timeUtc(2024, 11, 29, 12, 59), timeUtc(2024, 11, 29, 13, 0)},
		{29, months, timeUtc(2024, 11, 29, 23, 59), timeUtc(2028, 2, 29, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().
			OnMonthDay(d.cronDay).
			InMonths(d.cronMonths...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v", cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m * d m *
func TestCronMinuteDayMonth(t *testing.T) {
	data := []struct {
		cronMinute       int
		cronDay          int
		cronMonth        time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{59, 1, time.August, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 8, 1, 0, 59)},
		{59, 1, time.August, timeUtc(2024, 1, 31, 23, 59), timeUtc(2024, 8, 1, 0, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 0, 0), timeUtc(2024, 8, 1, 0, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 0, 59), timeUtc(2024, 8, 1, 1, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 1, 0), timeUtc(2024, 8, 1, 1, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 22, 30), timeUtc(2024, 8, 1, 22, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 22, 59), timeUtc(2024, 8, 1, 23, 59)},
		{59, 1, time.August, timeUtc(2024, 8, 1, 23, 59), timeUtc(2025, 8, 1, 0, 59)},
		{59, 1, time.August, timeUtc(2024, 12, 1, 12, 0), timeUtc(2025, 8, 1, 0, 59)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtMinute(d.cronMinute).
			OnMonthDay(d.cronDay).
			InMonth(d.cronMonth)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// m * d m,n *
func TestCronMinuteDayMonths(t *testing.T) {
	months := []time.Month{time.March, time.July}
	data := []struct {
		cronMinute       int
		cronDay          int
		cronMonths       []time.Month
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{44, 31, months, timeUtc(2024, 1, 1, 11, 0), timeUtc(2024, 3, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 2, 20, 10, 0), timeUtc(2024, 3, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 3, 25, 19, 0), timeUtc(2024, 3, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 3, 30, 23, 59), timeUtc(2024, 3, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 0, 0), timeUtc(2024, 3, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 0, 44), timeUtc(2024, 3, 31, 1, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 1, 0), timeUtc(2024, 3, 31, 1, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 1, 44), timeUtc(2024, 3, 31, 2, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 23, 0), timeUtc(2024, 3, 31, 23, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 23, 44), timeUtc(2024, 7, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 3, 31, 23, 59), timeUtc(2024, 7, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 4, 10, 10, 10), timeUtc(2024, 7, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 7, 1, 0, 0), timeUtc(2024, 7, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 7, 31, 0, 0), timeUtc(2024, 7, 31, 0, 44)},
		{44, 31, months, timeUtc(2024, 7, 31, 0, 44), timeUtc(2024, 7, 31, 1, 44)},
		{44, 31, months, timeUtc(2024, 7, 31, 3, 24), timeUtc(2024, 7, 31, 3, 44)},
		{44, 31, months, timeUtc(2024, 7, 31, 23, 24), timeUtc(2024, 7, 31, 23, 44)},
		{44, 31, months, timeUtc(2024, 7, 31, 23, 50), timeUtc(2025, 3, 31, 0, 44)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtMinute(d.cronMinute).
			OnMonthDay(d.cronDay).
			InMonths(d.cronMonths...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * * * w
func TestCronWeekday(t *testing.T) {
	data := []struct {
		cronWeekday      time.Weekday
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		// 2024-04-21 - Sunday
		{time.Sunday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 21, 11, 1)},
		{time.Monday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 22, 0, 0)},
		{time.Tuesday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 23, 0, 0)},
		{time.Wednesday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 24, 0, 0)},
		{time.Thursday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 25, 0, 0)},
		{time.Friday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 26, 0, 0)},
		{time.Saturday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 27, 0, 0)},
		{time.Sunday, timeUtc(2024, 4, 21, 23, 59), timeUtc(2024, 4, 28, 0, 0)},
		{time.Tuesday, timeUtc(2024, 5, 1, 14, 40), timeUtc(2024, 5, 7, 0, 0)},

		// 2024-04-28 - Sunday
		{time.Thursday, timeUtc(2024, 4, 28, 23, 45), timeUtc(2024, 5, 2, 0, 0)},
		{time.Thursday, timeUtc(2024, 5, 2, 23, 45), timeUtc(2024, 5, 2, 23, 46)},

		// 2024-01-01 - Monday
		{time.Monday, timeUtc(2023, 12, 28, 23, 45), timeUtc(2024, 1, 1, 0, 0)},
		{time.Saturday, timeUtc(2023, 12, 31, 23, 45), timeUtc(2024, 1, 6, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().OnWeekday(d.cronWeekday)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * * * w,v
func TestCronWeekdays(t *testing.T) {
	friMon := []time.Weekday{time.Friday, time.Monday}
	satSun := []time.Weekday{time.Saturday, time.Sunday}

	data := []struct {
		cronWeekdays     []time.Weekday
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		// 2024-04-21 - Sunday
		{friMon, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 22, 0, 0)},
		{friMon, timeUtc(2024, 4, 21, 23, 59), timeUtc(2024, 4, 22, 0, 0)},
		{friMon, timeUtc(2024, 4, 22, 0, 0), timeUtc(2024, 4, 22, 0, 1)},
		{friMon, timeUtc(2024, 4, 22, 1, 44), timeUtc(2024, 4, 22, 1, 45)},
		// 2024-04-26 - Friday
		{friMon, timeUtc(2024, 4, 22, 23, 59), timeUtc(2024, 4, 26, 0, 0)},
		{friMon, timeUtc(2024, 4, 23, 13, 59), timeUtc(2024, 4, 26, 0, 0)},
		{friMon, timeUtc(2024, 4, 24, 22, 22), timeUtc(2024, 4, 26, 0, 0)},
		{friMon, timeUtc(2024, 4, 26, 0, 0), timeUtc(2024, 4, 26, 0, 1)},
		{friMon, timeUtc(2024, 4, 26, 12, 12), timeUtc(2024, 4, 26, 12, 13)},
		{friMon, timeUtc(2024, 4, 26, 23, 59), timeUtc(2024, 4, 29, 0, 0)},

		{satSun, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 21, 11, 1)},
		{satSun, timeUtc(2024, 4, 21, 23, 59), timeUtc(2024, 4, 27, 0, 0)},
		{satSun, timeUtc(2024, 4, 27, 0, 0), timeUtc(2024, 4, 27, 0, 1)},
		{satSun, timeUtc(2024, 4, 27, 13, 59), timeUtc(2024, 4, 27, 14, 0)},
		{satSun, timeUtc(2024, 4, 27, 23, 59), timeUtc(2024, 4, 28, 0, 0)},
		{satSun, timeUtc(2024, 4, 28, 19, 19), timeUtc(2024, 4, 28, 19, 20)},
		{satSun, timeUtc(2024, 4, 28, 23, 59), timeUtc(2024, 5, 4, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().OnWeekdays(d.cronWeekdays...)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d * w
func TestCronDayWeekday(t *testing.T) {
	data := []struct {
		cronDom          int
		cronWeekday      time.Weekday
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		// 2024-04-21 - Sunday
		{22, time.Wednesday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 4, 22, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 21, 23, 59), timeUtc(2024, 4, 22, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 22, 0, 0), timeUtc(2024, 4, 22, 0, 1)},
		{22, time.Wednesday, timeUtc(2024, 4, 22, 13, 49), timeUtc(2024, 4, 22, 13, 50)},
		{22, time.Wednesday, timeUtc(2024, 4, 22, 23, 59), timeUtc(2024, 4, 24, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 23, 14, 0), timeUtc(2024, 4, 24, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 23, 23, 59), timeUtc(2024, 4, 24, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 24, 0, 0), timeUtc(2024, 4, 24, 0, 1)},
		{22, time.Wednesday, timeUtc(2024, 4, 24, 17, 12), timeUtc(2024, 4, 24, 17, 13)},

		// 2024-05-01 - Wednesday
		{22, time.Wednesday, timeUtc(2024, 4, 24, 23, 59), timeUtc(2024, 5, 1, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 27, 17, 18), timeUtc(2024, 5, 1, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 4, 30, 23, 59), timeUtc(2024, 5, 1, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 1, 0, 0), timeUtc(2024, 5, 1, 0, 1)},
		{22, time.Wednesday, timeUtc(2024, 5, 1, 15, 25), timeUtc(2024, 5, 1, 15, 26)},
		{22, time.Wednesday, timeUtc(2024, 5, 1, 23, 59), timeUtc(2024, 5, 8, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 2, 12, 0), timeUtc(2024, 5, 8, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 4, 12, 0), timeUtc(2024, 5, 8, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 7, 12, 0), timeUtc(2024, 5, 8, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 8, 23, 59), timeUtc(2024, 5, 15, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 10, 10, 59), timeUtc(2024, 5, 15, 0, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 15, 10, 59), timeUtc(2024, 5, 15, 11, 0)},
		{22, time.Wednesday, timeUtc(2024, 5, 15, 23, 59), timeUtc(2024, 5, 22, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().OnMonthDay(d.cronDom).OnWeekday(d.cronWeekday)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * * d m w
func TestCronDayMonthWeekday(t *testing.T) {
	data := []struct {
		cronDom          int
		cronMonth        time.Month
		cronWeekday      time.Weekday
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		/*
		           July                 August              September
		   Su Mo Tu We Th Fr Sa  Su Mo Tu We Th Fr Sa  Su Mo Tu We Th Fr Sa
		       1  2  3  4  5  6               1  2  3   1  2  3  4  5  6  7
		    7  8  9 10 11 12 13   4  5  6  7  8  9 10   8  9 10 11 12 13 14
		   14 15 16 17 18 19 20  11 12 13 14 15 16 17  15 16 17 18 19 20 21
		   21 22 23 24 25 26 27  18 19 20 21 22 23 24  22 23 24 25 26 27 28
		   28 29 30 31           25 26 27 28 29 30 31  29 30
		*/
		{3, time.August, time.Sunday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 7, 1, 11, 0), timeUtc(2024, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 1, 13, 0), timeUtc(2024, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 1, 13, 0), timeUtc(2024, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 2, 23, 59), timeUtc(2024, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 3, 0, 59), timeUtc(2024, 8, 3, 1, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 3, 19, 11), timeUtc(2024, 8, 3, 19, 12)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 3, 23, 59), timeUtc(2024, 8, 4, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 4, 0, 30), timeUtc(2024, 8, 4, 0, 31)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 4, 14, 30), timeUtc(2024, 8, 4, 14, 31)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 4, 23, 59), timeUtc(2024, 8, 11, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 6, 10, 10), timeUtc(2024, 8, 11, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 10, 10, 10), timeUtc(2024, 8, 11, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 11, 10, 10), timeUtc(2024, 8, 11, 10, 11)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 11, 23, 59), timeUtc(2024, 8, 18, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 13, 12, 12), timeUtc(2024, 8, 18, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 18, 23, 59), timeUtc(2024, 8, 25, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 25, 22, 59), timeUtc(2024, 8, 25, 23, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 8, 25, 23, 59), timeUtc(2025, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 9, 10, 10, 11), timeUtc(2025, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2024, 12, 31, 23, 59), timeUtc(2025, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2025, 2, 10, 23, 10), timeUtc(2025, 8, 3, 0, 0)},
		{3, time.August, time.Sunday, timeUtc(2025, 7, 31, 23, 10), timeUtc(2025, 8, 3, 0, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().
			OnMonthDay(d.cronDom).
			InMonth(d.cronMonth).
			OnWeekday(d.cronWeekday)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

// * h d m w
func TestCronHourDayMonthWeekday(t *testing.T) {
	data := []struct {
		cronHour         int
		cronDom          int
		cronMonth        time.Month
		cronWeekday      time.Weekday
		currentTime      time.Time
		expectedNextTime time.Time
	}{
		{21, 13, time.July, time.Friday, timeUtc(2024, 4, 21, 11, 0), timeUtc(2024, 7, 5, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 6, 30, 11, 0), timeUtc(2024, 7, 5, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 1, 23, 0), timeUtc(2024, 7, 5, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 5, 20, 42), timeUtc(2024, 7, 5, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 5, 21, 0), timeUtc(2024, 7, 5, 21, 1)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 5, 21, 59), timeUtc(2024, 7, 12, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 7, 7, 7), timeUtc(2024, 7, 12, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 12, 7, 7), timeUtc(2024, 7, 12, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 12, 21, 16), timeUtc(2024, 7, 12, 21, 17)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 12, 21, 59), timeUtc(2024, 7, 13, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 13, 21, 0), timeUtc(2024, 7, 13, 21, 1)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 13, 21, 33), timeUtc(2024, 7, 13, 21, 34)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 13, 21, 59), timeUtc(2024, 7, 19, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 15, 15, 59), timeUtc(2024, 7, 19, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 19, 3, 14), timeUtc(2024, 7, 19, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 19, 21, 50), timeUtc(2024, 7, 19, 21, 51)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 19, 21, 59), timeUtc(2024, 7, 26, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 19, 23, 59), timeUtc(2024, 7, 26, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 22, 12, 0), timeUtc(2024, 7, 26, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 26, 12, 0), timeUtc(2024, 7, 26, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 26, 21, 0), timeUtc(2024, 7, 26, 21, 1)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 7, 26, 21, 59), timeUtc(2025, 7, 4, 21, 0)},
		{21, 13, time.July, time.Friday, timeUtc(2024, 12, 31, 22, 13), timeUtc(2025, 7, 4, 21, 0)},
	}

	for _, d := range data {
		cronSched := NewCron().
			AtHour(d.cronHour).
			OnMonthDay(d.cronDom).
			InMonth(d.cronMonth).
			OnWeekday(d.cronWeekday)
		next := cronSched.Next(d.currentTime, nil)
		if !d.expectedNextTime.Equal(next) {
			t.Errorf("For cron %s and time %+v expected next %+v, but got %+v",
				cronSched.String(), d.currentTime, d.expectedNextTime, next)
		}
	}
}

func TestCronPartToString(t *testing.T) {
	data := []struct {
		input    []int
		expected string
	}{
		{[]int{}, "*"},
		{[]int{10}, "10"},
		{[]int{10, 8}, "10,8"},
	}

	for _, d := range data {
		res := cronPartToString(d.input)
		if res != d.expected {
			t.Errorf("For input %+v expected %s, but got: %s", d.input,
				d.expected, res)
		}
	}
}

func timeUtc(year, month, day, hour, minute int) time.Time {
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC)
}

func warsaw(t *testing.T) *time.Location {
	location, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Errorf("Cannot load Warsaw time location")
		return nil
	}
	return location
}
