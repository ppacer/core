package main

import (
	"go_shed/src/dag"
	"go_shed/src/db"
	"time"

	"github.com/rs/zerolog/log"
)

// This function is called on scheduler start up. TODO: more docs.
func start(dbClient *db.Client) {
	dagTasksSyncErr := syncDagTasks(dbClient)
	if dagTasksSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		log.Panic().Err(dagTasksSyncErr).Msg("Cannot sync up dag.registry and dagtasks")
	}
}

// TODO(dskrzypiec): docs
func syncDagTasks(dbClient *db.Client) error {
	start := time.Now()
	log.Info().Msg("Start syncing dag.registry and dagtasks table...")
	for _, dagId := range dag.List() {
		d, _ := dag.Get(dagId)

		// TODO(dskrzypiec): Check DAG hash before inserting new version of dagtasks!
		sErr := dbClient.InsertDagTasks(d)
		if sErr != nil {
			log.Error().Err(sErr).Str("dagId", string(dagId)).Msg("Could not sync dag with dagtasks")
		}
	}
	log.Info().Dur("durationMs", time.Since(start)).Msg("Finished syncing dag.Registry and dagtasks table")
	return nil
}
