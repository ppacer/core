package timeutils

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

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
