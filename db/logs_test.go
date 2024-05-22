package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ppacer/core/timeutils"
)

func TestInsertTaskLogSimple(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs(nil)
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

func TestReadDagRunTaskLogsAllEmptyTable(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	const dagId = "mock_dag"
	const taskId = "task_1"

	// Read all log records for the task
	ctx := context.Background()
	records, rErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if rErr != nil {
		t.Errorf("Error while reading DAG run [%s] task [%s] logs: %s",
			dagId, taskId, rErr.Error())
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 log records, got: %d", len(records))
	}
}

func TestReadDagRunTaskLogsAll(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	const dagId = "mock_dag"
	const taskId = "task_1"

	// Insert loggs for a single DAG run task
	msgs := []struct {
		Msg  string
		Attr string
	}{
		{"Msg 1", "{}"},
		{"Another one", `{"x": 42}`},
		{"The last one", `{"z": ["x", "y", "z"]}`},
	}

	for _, msg := range msgs {
		tlr := TaskLogRecord{
			DagId:      dagId,
			ExecTs:     execTs,
			TaskId:     taskId,
			InsertTs:   timeutils.ToString(time.Now()),
			Level:      "INFO",
			Message:    msg.Msg,
			Attributes: msg.Attr,
		}
		iErr := c.InsertTaskLog(tlr)
		if iErr != nil {
			t.Errorf("Error while inserting %v: %s", tlr, iErr.Error())
		}
	}

	// Read all log records for the task
	ctx := context.Background()
	records, rErr := c.ReadDagRunTaskLogs(ctx, dagId, execTs, taskId)
	if rErr != nil {
		t.Errorf("Error while reading DAG run [%s] task [%s] logs: %s",
			dagId, taskId, rErr.Error())
	}
	if len(records) != len(msgs) {
		t.Errorf("Expected %d log records, got: %d", len(msgs), len(records))
	}

	for idx, rec := range records {
		if rec.Message != msgs[idx].Msg {
			t.Errorf("For row %d expected message [%s], but got: [%s]", idx,
				msgs[idx].Msg, rec.Message)
		}
		if rec.Attributes != msgs[idx].Attr {
			t.Errorf("For row %d expected Attributes [%s], but got: [%s]", idx,
				msgs[idx].Attr, rec.Attributes)
		}
	}
}

func TestReadDagRunTaskLatest(t *testing.T) {
	c, err := NewSqliteTmpClientForLogs(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ts := time.Now()
	execTs := timeutils.ToString(ts)
	const dagId = "mock_dag"
	const taskId = "task_1"

	// Insert loggs for a single DAG run task
	msgs := []struct {
		Msg  string
		Attr string
	}{
		{"Msg 1", "{}"},
		{"Another one", `{"x": 42}`},
		{"The last one", `{"z": ["x", "y", "z"]}`},
	}

	for _, msg := range msgs {
		tlr := TaskLogRecord{
			DagId:      dagId,
			ExecTs:     execTs,
			TaskId:     taskId,
			InsertTs:   timeutils.ToString(time.Now()),
			Level:      "INFO",
			Message:    msg.Msg,
			Attributes: msg.Attr,
		}
		iErr := c.InsertTaskLog(tlr)
		if iErr != nil {
			t.Errorf("Error while inserting %v: %s", tlr, iErr.Error())
		}
	}

	// Read only the latest log record
	ctx := context.Background()
	records, rErr := c.ReadDagRunTaskLogsLatest(ctx, dagId, execTs, taskId, 1)
	if rErr != nil {
		t.Errorf("Error while reading DAG run [%s] task [%s] logs: %s",
			dagId, taskId, rErr.Error())
	}
	if len(records) != 1 {
		t.Errorf("Expected to get only the latest log records, but got: %d",
			len(records))
	}

	latestExpectedMsg := msgs[len(msgs)-1]
	if records[0].Message != latestExpectedMsg.Msg {
		t.Errorf("Expected message [%s], got [%s]", latestExpectedMsg.Msg,
			records[0].Message)
	}

	// Read 2 latest log records
	records, rErr = c.ReadDagRunTaskLogsLatest(ctx, dagId, execTs, taskId, 2)
	if rErr != nil {
		t.Errorf("Error while reading DAG run [%s] task [%s] logs: %s",
			dagId, taskId, rErr.Error())
	}
	if len(records) != 2 {
		t.Errorf("Expected to read latest 2 rows, but got: %d", len(records))
	}

	for idx, rec := range records {
		msgsIdx := idx + 1
		if rec.Message != msgs[msgsIdx].Msg {
			t.Errorf("For row %d expected message [%s], but got: [%s]", idx,
				msgs[msgsIdx].Msg, rec.Message)
		}
		if rec.Attributes != msgs[msgsIdx].Attr {
			t.Errorf("For row %d expected Attributes [%s], but got: [%s]", idx,
				msgs[msgsIdx].Attr, rec.Attributes)
		}
	}
}

func BenchmarkInsertTaskLog(b *testing.B) {
	c, err := NewSqliteTmpClientForLogs(nil)
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
