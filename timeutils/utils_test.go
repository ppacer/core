// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package timeutils

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

func TestDefaultTimezone(t *testing.T) {
	if ppacerTimezone != time.Local {
		t.Errorf("Expected default timezone, to be Local, got: %v",
			ppacerTimezone)
	}
}

func TestSetTimezone(t *testing.T) {
	for _, tzName := range timezoneNames() {
		setErr := SetTimezone(tzName)
		if setErr != nil {
			t.Errorf("Failed setting up timezone to %s: %s",
				tzName, setErr.Error())
			continue
		}
		now := Now()
		if now.Location().String() != tzName {
			t.Errorf("Expected tiemstamp from %s timezone, got: %v", tzName,
				now)
		}
	}
	setErr := SetTimezone("Local")
	if setErr != nil {
		t.Errorf("Cannot set timezone back to Local: %s", setErr.Error())
	}
}

func TestCurrentTz(t *testing.T) {
	for _, tzName := range timezoneNames() {
		setErr := SetTimezone(tzName)
		if setErr != nil {
			t.Errorf("Failed setting up timezone to %s: %s",
				tzName, setErr.Error())
			continue
		}
		now := time.Now().In(CurrentTz())
		if now.Location().String() != tzName {
			t.Errorf("Expected timestamp in %s timezone, got: %v", tzName, now)
		}
	}
}

func TestToStringBasic(t *testing.T) {
	warsawTz := warsawTimeZone(t)
	tss := []time.Time{
		time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC),
		time.Date(2023, time.August, 22, 15, 10, 5, 123456000, time.UTC),
		time.Date(2023, time.August, 22, 15, 10, 5, 123456000, warsawTz),
		time.Date(2023, time.November, 11, 17, 8, 0, 0, time.UTC),
	}
	expected := []string{
		"2023-08-22T15:00:00UTC+00:00",
		"2023-08-22T15:10:05.123456UTC+00:00",
		"2023-08-22T15:10:05.123456CEST+02:00",
		"2023-11-11T17:08:00UTC+00:00",
	}

	for idx, ts := range tss {
		s := ToString(ts)
		e := expected[idx]
		if s != e {
			t.Errorf("Expected ToString(%v)=%s, got: %s", ts, e, s)
		}
	}
}

func TestToDateUTCStringBasic(t *testing.T) {
	warsawTz := warsawTimeZone(t)
	tss := []time.Time{
		time.Date(2023, time.August, 22, 23, 0, 0, 0, time.UTC),
		time.Date(2023, time.August, 23, 1, 0, 0, 0, warsawTz),
		time.Date(2023, time.August, 23, 15, 10, 5, 123456000, warsawTz),
		time.Date(2024, time.November, 11, 17, 8, 0, 0, time.UTC),
	}
	expected := []string{
		"2023-08-22",
		"2023-08-22",
		"2023-08-23",
		"2024-11-11",
	}

	for idx, ts := range tss {
		s := ToDateUTCString(ts)
		e := expected[idx]
		if s != e {
			t.Errorf("Expected ToDateUTCString(%v)=%s, got: %s", ts, e, s)
		}
	}
}

func TestFromStringSimple(t *testing.T) {
	warsawTz := warsawTimeZone(t)
	inputs := []string{
		"2023-08-22T15:00:00UTC+00:00",
		"2023-08-22T15:10:05.123456UTC+00:00",
		"2023-08-22T15:10:05.123456CEST+02:00",
		"2023-08-22T15:10:05.100000CEST+02:00",
		"2023-08-22T15:10:05.10000CEST+02:00",
		"2023-08-22T15:10:05.1000CEST+02:00",
		"2023-08-22T15:10:05.100CEST+02:00",
		"2023-08-22T15:10:05.10CEST+02:00",
		"2023-08-22T15:10:05.1CEST+02:00",
	}
	expected := []time.Time{
		time.Date(2023, time.August, 22, 15, 0, 0, 0, time.UTC),
		time.Date(2023, time.August, 22, 15, 10, 5, 123456000, time.UTC),
		time.Date(2023, time.August, 22, 15, 10, 5, 123456000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
		time.Date(2023, time.August, 22, 15, 10, 5, 100000000, warsawTz),
	}

	for idx, str := range inputs {
		ts, err := FromString(str)
		if err != nil {
			t.Errorf("Could not parse string to time.Time for %s: %s", str, err.Error())
		}
		e := expected[idx]
		if !ts.Equal(e) {
			t.Errorf("Expected %v, got %v", e, ts)
		}
	}
}

func TestFromStringNotTime(t *testing.T) {
	incorrectInputs := []string{
		"Damian",
		"test",
		"",
		"2023-02-29T12:00:00.123CEST+02:00",
		"2023-13-29T12:00:00.123CEST+02:00",
		"2023-13-50T12:00:00.123CEST+02:00",
		"2023-12-10T25:10:00.123CEST+02:00",
		"2023-12-10T22:70:00.123CEST+02:00",
		"2023-12-10T22:10:90.123CEST+02:00",
	}
	for _, input := range incorrectInputs {
		ts, err := FromString(input)
		if err == nil {
			t.Errorf("Expected error while parsing incorrect timestamp string %s, but it's fine: %v", input, ts)
		}
	}
}

func TestTimeToJsonBackToTime(t *testing.T) {
	type tmp struct {
		T string `json:"time"`
	}
	t1 := time.Date(2023, time.August, 22, 15, 10, 5, 123456000, time.UTC)
	tmp1 := tmp{T: ToString(t1)}
	json1, jErr1 := json.Marshal(tmp1)
	if jErr1 != nil {
		t.Errorf("Cannot marshal tmp %v: %s", tmp1, jErr1.Error())
	}
	var fromJsonTmp tmp
	unmarshalErr := json.Unmarshal(json1, &fromJsonTmp)
	if unmarshalErr != nil {
		t.Errorf("Error while unmarshaling from tmp JSON %s: %s", string(json1),
			unmarshalErr.Error())
	}
	t1FromStr, parseErr := FromString(fromJsonTmp.T)
	if parseErr != nil {
		t.Errorf("Cannot do FromString from %s: %s", fromJsonTmp.T,
			parseErr.Error())
	}
	if t1.Compare(t1FromStr) != 0 {
		t.Errorf("Expected %v, got %v", t1, t1FromStr)
	}
}

/*
func TestNowToAndFromString(t *testing.T) {
	// TODO: To be fixed. We need to handle monotonic clock readings
	// From CI: FromString(ToString(2023-09-28 21:46:48.734831261 +0000 UTC m=+0.009153610))!=2023-09-28 21:46:48.734831 +0000 UTC
	ts := time.Now()
	str := ToString(ts)
	tsFromStr, err := FromString(str)
	if err != nil {
		t.Errorf("Could not parse %s to time.Time: %s", str, err.Error())
	}
	if tsFromStr.Compare(ts) != 0 {
		t.Errorf("FromString(ToString(%v))!=%v", ts, tsFromStr)
	}
}
*/

func TestFuzzToAndFromString(t *testing.T) {
	const N = 100
	warsawTz := warsawTimeZone(t)
	for i := 0; i < N; i++ {
		randTs := randomTime(warsawTz)
		str := ToString(randTs)
		tsFromStr, err := FromString(str)
		if err != nil {
			t.Errorf("Could not parse %s to time.Time: %s", str, err.Error())
		}
		if !tsFromStr.Equal(randTs) {
			t.Errorf("FromString(ToString(%v))!=%v", randTs, tsFromStr)
		}
	}
}

func TestFuzzToAndFromStringMust(t *testing.T) {
	const N = 100
	warsawTz := warsawTimeZone(t)
	for i := 0; i < N; i++ {
		randTs := randomTime(warsawTz)
		str := ToString(randTs)
		tsFromStr := FromStringMust(str)
		if !tsFromStr.Equal(randTs) {
			t.Errorf("FromString(ToString(%v))!=%v", randTs, tsFromStr)
		}
	}
}

func TestFromStringMustFailed(t *testing.T) {
	incorrectTimestampStrings := []string{
		"2023-08-22T15:00:00+00:00",
		"2023-08-22 15:10:05.123456UTC+00:00",
		"2023-08-22_15:10:05.123456CEST+02:00",
		"2023-11-11 17:08:00",
		"",
		"notatimestamp",
	}
	expected := time.Time{}

	for _, tsStr := range incorrectTimestampStrings {
		ts := FromStringMust(tsStr)
		if ts != expected {
			t.Errorf("Expected empty time.Time{}, got: %v", ts)
		}
	}
}

func TestToStringUI(t *testing.T) {
	t0 := time.Now()
	t1 := time.Date(t0.Year(), t0.Month(), t0.Day(), 8, 59, 48, 987000000,
		time.UTC)
	dayBefore := t1.Add(-24 * time.Hour)
	dateBefore := dayBefore.Format(UiDateFormat)

	tests := []struct {
		input    time.Time
		expected string
	}{
		{t1, "08:59:48"},
		{dayBefore, dateBefore + " 08:59:48"},
	}

	for _, test := range tests {
		res := ToStringUI(test.input)
		if res != test.expected {
			t.Errorf("Expected ToStringUI(%v) = %s, got: %s", test.input,
				test.expected, res)
		}
	}
}

func BenchmarkStringToStringUI(b *testing.B) {
	x := time.Now()
	xStr := ToString(x)

	for i := 0; i < b.N; i++ {
		ToStringUI(FromStringMust(xStr))
	}
}

func BenchmarkFromStringMust(b *testing.B) {
	x := time.Now()
	xStr := ToString(x)

	for i := 0; i < b.N; i++ {
		FromStringMust(xStr)
	}
}

func warsawTimeZone(t *testing.T) *time.Location {
	location, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatalf("Cannot load Warsaw timezone location: %s", err.Error())
	}
	return location
}

func randomTime(tz *time.Location) time.Time {
	year := rand.Intn(2023-1900) + 1900
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1

	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	ns := rand.Intn(10000000) * 1000

	return time.Date(year, time.Month(month), day, hour, minute, second, ns, tz)
}

func timezoneNames() []string {
	return []string{
		"Europe/Warsaw",
		"Europe/Kiev",
		"Europe/London",
		"America/Bogota",
		"America/Costa_Rica",
		"America/Denver",
		"America/Los_Angeles",
		"America/New_York",
		"Asia/Kamchatka",
		"Asia/Kolkata",
		"Asia/Pyongyang",
		"Asia/Seoul",
		"Asia/Vladivostok",
		"Local",
	}
}
