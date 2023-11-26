package user

import (
	"log/slog"

	"github.com/dskrzypiec/scheduler/src/dag"
)

func init() {
	// TODO(dskrzypiec): Consider dynamically parsing function which uses
	// dag.Add call and call it on init via reflection. For now though it's
	// good enough to manually add on init.
	err := dag.Add(createHelloDag())
	if err != nil {
		slog.Error("Could not add hello DAG", "err", err)
	}
	err = dag.Add(createPrintDag())
	if err != nil {
		slog.Error("Could not add print DAG", "err", err)
	}
	err = dag.Add(createRandomFailDag(10))
	if err != nil {
		slog.Error("Could not add random failing DAG", "err", err)
	}
}
