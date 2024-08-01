package core

import (
	"context"
	"fmt"
	"log"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/exec"
	"github.com/ppacer/core/scheduler"
)

// DefaultStarted setups default Scheduler, Executor (in separate
// goroutines) and starts Scheduler HTTP server. This function is meant to
// reduce boilerplate for simple examples and tests. When there is an error on
// starting Scheduler server this function panics.
func DefaultStarted(ctx context.Context, dags dag.Registry, port int) {
	const (
		db     = "scheduler.db"
		logsDb = "logs.db"
	)

	schedulerServer := scheduler.DefaultStarted(ctx, dags, db, port)

	// Setup and run executor in a separate goroutine
	go func() {
		schedUrl := fmt.Sprintf("http://localhost:%d", port)
		executor := exec.NewDefault(schedUrl, logsDb)
		executor.Start(dags)
	}()

	// Start scheduler HTTP server
	lasErr := schedulerServer.ListenAndServe()
	if lasErr != nil {
		log.Panicf("Cannot start the server: %s\n", lasErr.Error())
	}
}
