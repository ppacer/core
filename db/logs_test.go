package db

import (
	"context"
	"fmt"
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
	execTs := timeutils.ToString(ts)
	const dagId = "mock_dag"
	const taskId = "task_1"

	tlr := TaskLogRecord{
		DagId:      dagId,
		ExecTs:     execTs,
		TaskId:     taskId,
		InsertTs:   execTs,
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

	ctx := context.Background()
	tlrs, readErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if readErr != nil {
		t.Errorf("Error while reading DAG run task logs: %s", readErr.Error())
	}
	if len(tlrs) != 1 {
		t.Errorf("Expected to read 1 task log record, got: %d", len(tlrs))
	}
	tlrDb := tlrs[0]
	if tlr != tlrDb {
		t.Errorf("Read from DB task log record %+v differs from initial version: %+v",
			tlrDb, tlr)
	}
}

func BenchmarkInsertTaskLog(b *testing.B) {
	c, err := NewSqliteTmpClientForLogs()
	if err != nil {
		b.Fatal(err)
	}
	const dagId = "mock_dag"
	const taskId = "task"
	ts := time.Now()
	execTs := timeutils.ToString(ts)

	tlr := TaskLogRecord{
		DagId:      dagId,
		ExecTs:     execTs,
		TaskId:     taskId,
		InsertTs:   execTs,
		Level:      "INFO",
		Message:    "Test message",
		Attributes: `{"x": 12, "y": "test"}`,
	}

	for i := 0; i < b.N; i++ {
		tlr.TaskId = fmt.Sprintf("%s_%d", taskId, i)
		iErr := c.InsertTaskLog(tlr)
		if iErr != nil {
			b.Fail()
		}
	}
}
