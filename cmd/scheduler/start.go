package main

import (
	"database/sql"
	"go_shed/src/dag"
	"go_shed/src/db"
	"time"

	"github.com/rs/zerolog/log"
)

// This function is called on scheduler start up. TODO: more docs.
func start(dbClient *db.Client) {
	dagTasksSyncErr := syncDags(dbClient)
	if dagTasksSyncErr != nil {
		// TODO(dskrzypiec): what now? Probably retries... and eventually panic
		log.Panic().Err(dagTasksSyncErr).Msg("Cannot sync up dag.registry and dagtasks")
	}
}

// Synchronize all DAGs from dag.registry with dags and dagtasks tables in the database. If only DAG attributes were
// changed then the record in dags table would be updated and dagtasks would not.
func syncDags(dbClient *db.Client) error {
	start := time.Now()
	log.Info().Msg("Start syncing dag.registry with dags and dagtasks tables...")
	for _, d := range dag.List() {
		syncErr := syncDag(dbClient, d)
		if syncErr != nil {
			// TODO(dskrzypiec): Probably we should retunr []error and don't stop when syncing one DAG would fail
			// This should be solved together with general approach to error handling in the scheduler.
			return syncErr
		}
	}
	log.Info().Dur("durationMs", time.Since(start)).Msg("Finished syncing dag.Registry with dags and dagtasks tables")
	return nil
}

// Synchronize given DAG with dags and dagtasks tables in the database.
func syncDag(dbClient *db.Client, d dag.Dag) error {
	dagId := string(d.Id)
	dbDag, dbRErr := dbClient.ReadDag(dagId)
	if dbRErr != nil && dbRErr != sql.ErrNoRows {
		log.Error().Err(dbRErr).Str("dagId", dagId).Msgf("Could not get DAg metadata from dags table")
		return dbRErr
	}
	if dbRErr == sql.ErrNoRows {
		// There is no DAG in the database yet
		uErr := dbClient.UpsertDag(d)
		if uErr != nil {
			log.Error().Err(uErr).Str("dagId", dagId).Msgf("Could no insert DAG into dags table")
			return uErr
		}
		dtErr := dbClient.InsertDagTasks(d)
		if dtErr != nil {
			log.Error().Err(dtErr).Str("dagId", dagId).Msgf("Could not insert DAG tasks into dagtasks table")
			return dtErr
		}
		return nil
	}
	// Check if update is needed
	if dbDag.HashDagMeta == d.HashDagMeta() && dbDag.HashTasks == d.HashTasks() {
		log.Debug().Str("dagId", dagId).Msg("There is no need for update. HashAttr and HashTasks are not changed")
		return nil
	}
	// Either attributes or tasks for this DAG were modified
	uErr := dbClient.UpsertDag(d)
	if uErr != nil {
		log.Error().Err(uErr).Str("dagId", dagId).Msgf("Could no update DAG in dags table")
		return uErr
	}
	if dbDag.HashTasks != d.HashTasks() {
		log.Info().Str("dagId", dagId).Str("prevHashTasks", dbDag.HashTasks).Str("currHashTasks", d.HashTasks()).
			Msg("Hash tasks has changed. New version of dagtasks will be inserted.")
		dtErr := dbClient.InsertDagTasks(d)
		if dtErr != nil {
			log.Error().Err(dtErr).Str("dagId", dagId).Msgf("Could not insert DAG tasks into dagtasks table")
			return dtErr
		}
	}
	return nil
}
