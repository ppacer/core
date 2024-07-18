package db

import "testing"

func TestSqliteTmpClientForLogsCreation(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs(testLogger())
	if err != nil {
		t.Fatalf("Cannot create Client for logs: %s", err.Error())
	}
	defer CleanUpSqliteTmp(c, t)

	rows := c.Count("tasklogs")
	if rows != 0 {
		t.Errorf("Expected table logs to exists and has 0 records, got: %d",
			rows)
	}
}
