package db

import (
	"testing"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestInsertTaskLogSimple(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs()
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ts := time.Now()

	tlr := TaskLogRecord{
		Date:       timeutils.ToDateUTCString(ts),
		DagId:      "mock_dag",
		ExecTs:     timeutils.ToString(ts),
		TaskId:     "task_1",
		InsertTs:   timeutils.ToString(ts),
		LogTs:      timeutils.ToString(ts),
		Level:      "INFO",
		Message:    "Test message",
		Attributes: `{"x": 12, "y": "test"}`,
	}

	rowsBefore := c.Count("tasklogs")
	if rowsBefore != 0 {
		t.Errorf("Expected 0 ros in tasklogs, got: %d", rowsBefore)
	}

	iErr := c.InsertTaskLog(tlr)
	if iErr != nil {
		t.Errorf("Error while inserting task log record: %s", iErr.Error())
	}

	rowsAfter := c.Count("tasklogs")
	if rowsAfter != 1 {
		t.Errorf("Expected exactly 1 log record, got: %d", rowsAfter)
	}
}
