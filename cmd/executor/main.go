package main

import (
	"github.com/dskrzypiec/scheduler/exec"
	_ "github.com/dskrzypiec/scheduler/user"
)

func main() {
	cfg := ParseConfig()
	cfg.setupLogger()

	executor := exec.New(cfg.SchedulerUrl, nil)
	executor.Start()
}
