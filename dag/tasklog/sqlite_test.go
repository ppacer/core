package tasklog

import (
	"testing"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/db"
)

func TestSQLiteLoggerSimple(t *testing.T) {
	c, err := db.NewSqliteTmpClientForLogs()
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	ri := dag.RunInfo{DagId: dag.Id("mock_dag"), ExecTs: time.Now()}
	const taskId = "task_1"
	l := NewSQLiteLogger(ri, taskId, c, nil)

	l.Error("test message", "val1", 42, "name", "Damian")

	// t.Error("just to test manually what have been written into SQLite DB")
}
