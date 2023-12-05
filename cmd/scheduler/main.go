package main

import (
	"fmt"
	"net/http"

	"log"
	"log/slog"

	_ "github.com/dskrzypiec/scheduler/user"

	"github.com/dskrzypiec/scheduler/db"
	"github.com/dskrzypiec/scheduler/sched"
)

func main() {
	cfg := ParseConfig()
	cfg.setupLogger()
	dbClient, err := db.NewSqliteClient("/tmp/test_ds.db")
	if err != nil {
		slog.Error("Cannot connect to the database", "err", err)
		log.Panic("Cannot connect to the database")
	}
	scheduler := sched.New(dbClient)
	httpHandler := scheduler.Start()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: httpHandler,
	}

	slog.Info("Starting scheduler", "version", cfg.AppVersion, "port", cfg.Port)
	lasErr := server.ListenAndServe()
	if lasErr != nil {
		slog.Error("ListenAndServer failed", "err", lasErr)
		log.Panic("Cannot start the server")
	}
}
