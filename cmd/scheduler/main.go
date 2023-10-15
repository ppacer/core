package main

import (
	"fmt"
	"go_shed/src/db"
	"go_shed/src/sched"
	_ "go_shed/src/user"
	"net/http"

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
