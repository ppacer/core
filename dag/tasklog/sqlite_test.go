package tasklog

import (
	"context"
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
	"github.com/ppacer/core/timeutils"
)

func TestSQLiteLoggerSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs()
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	const dagId = "mock_dag"
	const taskId = "task_1"
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	ri := dag.RunInfo{DagId: dag.Id(dagId), ExecTs: ts}
	sqliteLogger := NewSQLiteLogger(ri, taskId, c, nil)

	messages := []string{
		"test message 1",
		"",
		"test message 2",
	}
	for _, msg := range messages {
		sqliteLogger.Error(msg)
	}

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if readErr != nil {
		t.Errorf("Error when reading tasklogs from database: %s", readErr.Error())
	}
	if len(tlrs) != len(messages) {
		t.Errorf("Expected %d logs in tasklogs, got: %d", len(messages),
			len(tlrs))
	}
	for idx, msg := range messages {
		if tlrs[idx].Message != msg {
			t.Errorf("For log %d expected message [%s], got [%s]",
				idx, msg, tlrs[idx].Message)
		}
	}
}

// TODO(dskrzypiec): Test for inserting attributes (several types)

// TODO(dskrzypiec): Test for severity level of the logger
