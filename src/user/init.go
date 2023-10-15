package user

import (
	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/rs/zerolog/log"
)

func init() {
	// TODO(dskrzypiec): Consider dynamically parsing function which uses dag.Add call and call it on init via
	// reflection. For now though it's good enough to manually add on init.
	err := dag.Add(createHelloDag())
	if err != nil {
		log.Error().Err(err).Msg("Could not add hello DAG")
	}
	err = dag.Add(createPrintDag())
	if err != nil {
		log.Error().Err(err).Msg("Could not add print DAG")
	}
}
