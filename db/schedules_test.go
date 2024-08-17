package db

import (
	"context"
	"testing"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestScheduleInsertAndRead(t *testing.T) {
	c, err := NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Error(err)
	}
	defer CleanUpSqliteTmp(c, t)
	const tableName = "schedules"

	c1 := c.Count(tableName)
	if c1 != 0 {
		t.Errorf("Expected 0 rows in %s initially, got: %d", tableName, c1)
	}

	ctx := context.Background()
	const (
		dagId = "sample_dag"
		event = "regular"
	)
	sched := timeutils.ToString(time.Now())
	iErr := c.InsertDagSchedule(ctx, dagId, event, sched, nil)
	if iErr != nil {
		t.Errorf("Error while inserting new schedule: %s", iErr.Error())
	}

	c2 := c.Count(tableName)
	if c2 != 1 {
		t.Errorf("Expected 1 row in %s initially, got: %d", tableName, c2)
	}

	schedules, rErr := c.ReadDagSchedules(ctx, dagId)
	if rErr != nil {
		t.Errorf("Error while reading schedules for dagId=%s: %s",
			dagId, rErr.Error())
	}
	if len(schedules) != 1 {
		t.Errorf("Expected 1 schedule for dagId=%s, got: %d",
			dagId, len(schedules))
	}
	expected := Schedule{
		DagId:          dagId,
		InsertTs:       schedules[0].InsertTs,
		Event:          event,
		ScheduleTs:     nil,
		NextScheduleTs: sched,
	}
	if schedules[0] != expected {
		t.Errorf("Expected schedule %+v, got: %+v",
			expected, schedules[0])
	}
}
