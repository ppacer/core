package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ppacer/core/timeutils"
)

func TestReadDagRunRestartLatestEmpty(t *testing.T) {
	c, err := NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ctx := context.Background()
	_, rErr := c.ReadDagRunRestartLatest(ctx, "any-dag-id", "any-exec-ts")
	if rErr != sql.ErrNoRows {
		t.Errorf("Expected ErrNoRows, got %v", rErr)
	}
}

func TestInsertAndReadDagRunRestartLatest(t *testing.T) {
	c, err := NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ctx := context.Background()
	const dagId = "sample_dag_id"
	execTs := timeutils.ToString(timeutils.Now())

	iErr := c.InsertDagRunNextRestart(ctx, dagId, execTs)
	if iErr != nil {
		t.Errorf("Cannot insert new DAG run restart event: %v", iErr)
	}

	drr, rErr := c.ReadDagRunRestartLatest(ctx, dagId, execTs)
	if rErr != nil {
		t.Errorf("Cannot read new DAG run restart event: %v", rErr)
	}
	if drr.Restart != 1 {
		t.Errorf("Expected restart=1, got %d in (%+v)", drr.Restart, drr)
	}
}

func TestInsertAndReadDagRunRestartLatestMany(t *testing.T) {
	const N = 13
	c, err := NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ctx := context.Background()
	const dagId = "sample_dag_id"
	execTs := timeutils.ToString(timeutils.Now())

	for i := 0; i < N; i++ {
		iErr := c.InsertDagRunNextRestart(ctx, dagId, execTs)
		if iErr != nil {
			t.Errorf("Cannot insert new DAG run restart event: %v", iErr)
		}
	}

	drr, rErr := c.ReadDagRunRestartLatest(ctx, dagId, execTs)
	if rErr != nil {
		t.Errorf("Cannot read new DAG run restart event: %v", rErr)
	}
	if drr.Restart != N {
		t.Errorf("Expected restart=%d, got %d in (%+v)", N, drr.Restart, drr)
	}
}

func TestInsertAndReadDagRunRestartLatestManyMany(t *testing.T) {
	c, err := NewSqliteTmpClient(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer CleanUpSqliteTmp(c, t)
	ctx := context.Background()
	execTs := timeutils.ToString(timeutils.Now())

	input := []struct {
		dagId    string
		restarts int
	}{
		{"dag1", 1},
		{"dag2", 7},
		{"dag3", 2},
		{"dag4", 1},
	}

	for _, in := range input {
		for i := 0; i < in.restarts; i++ {
			iErr := c.InsertDagRunNextRestart(ctx, in.dagId, execTs)
			if iErr != nil {
				t.Errorf("Cannot insert new DAG run restart event: %v", iErr)
			}
		}
	}

	for _, in := range input {
		drr, rErr := c.ReadDagRunRestartLatest(ctx, in.dagId, execTs)
		if rErr != nil {
			t.Errorf("Cannot read new DAG run restart event: %v", rErr)
		}
		if drr.Restart != in.restarts {
			t.Errorf("Expected restart=%d, got %d in (%+v)", in.restarts,
				drr.Restart, drr)
		}
	}
}
