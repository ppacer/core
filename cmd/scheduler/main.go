package main

import (
	"fmt"
	"net/http"

	_ "github.com/dskrzypiec/scheduler/src/user"

	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/sched"
	"github.com/rs/zerolog/log"
)

func main() {
	cfg := ParseConfig()
	cfg.setupZerolog()
	dbClient, err := db.NewClient("/Users/ds/GoProjects/go_sched/test.db")
	if err != nil {
		log.Panic().Err(err).Msg("Cannot connect to the database")
	}
	scheduler := sched.New(dbClient)
	httpHandler := scheduler.Start()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: httpHandler,
	}

	log.Info().Msgf("Start Scheduler v%s on :%d...", cfg.AppVersion, cfg.Port)
	lasErr := server.ListenAndServe()
	if lasErr != nil {
		log.Panic().Err(lasErr).Msg("Cannot start the server")
	}
}
