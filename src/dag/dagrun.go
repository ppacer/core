package dag

import (
	"time"
)

type DagRun struct {
	RunId    int64
	DagId    Id
	ExecTs   time.Time
	InsertTs time.Time
	Status   string
	Version  string
}

/*
TODO(dskrzypiec): For now src/db depends on src/dag...
func fromDbDagRun() DagRun {
	return DagRun{
		RunId:    dr.RunId,
		DagId:    Id(dr.DagId),
		ExecTs:   time.Time{}, // TODO
		InsertTs: time.Time{}, // TODO
		Status:   dr.Status,
		Version:  dr.Version,
	}
}
*/
