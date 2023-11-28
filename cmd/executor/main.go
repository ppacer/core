package main

import (
	"github.com/dskrzypiec/scheduler/src/exec"
	_ "github.com/dskrzypiec/scheduler/src/user"
)

func main() {
	cfg := ParseConfig()
	cfg.setupLogger()

	executor := exec.New(cfg.SchedulerUrl, nil)
	executor.Start()
}
