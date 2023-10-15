package main

import (
	"fmt"
	"time"

	"github.com/dskrzypiec/scheduler/cmd/executor/sched_client"
	"github.com/dskrzypiec/scheduler/src/dag"
	"github.com/rs/zerolog/log"

	_ "github.com/dskrzypiec/scheduler/src/user"
)

func main() {
	cfg := ParseConfig()
	cfg.setupZerolog()

	const dagId = "hello_dag"
	helloDag, err := dag.Get(dag.Id(dagId))
	if err != nil {
		log.Error().Err(err).Msg("Could not get DAG hello_dag from registry")
	}
	fmt.Printf("hello_dag.%s.Execute() source:\n", helloDag.Root.Task.Id())
	fmt.Println(dag.TaskExecuteSource(helloDag.Root.Task))
	fmt.Println("SHA256:")
	fmt.Println(helloDag.Root.Hash())

	fmt.Println("Executing...")
	fmt.Printf("Task %s:\n", helloDag.Root.Task.Id())
	helloDag.Root.Task.Execute()
	for _, ch := range helloDag.Root.Children {
		fmt.Printf("Task %s:\n", ch.Task.Id())
		ch.Task.Execute()
	}

	schedClient := sched_client.New("http://localhost:8080", nil)

	for {
		tte, err := schedClient.GetTask()
		if err != nil {
			log.Error().Err(err)
			break
		}
		fmt.Printf("Executing %s.%s.Execute()...\n", tte.DagId, tte.TaskId)
		d, dErr := dag.Get(dag.Id(tte.DagId))
		if dErr != nil {
			log.Error().Err(dErr).Msgf("Could not get DAG from registry: %s", tte.DagId)
			break
		}
		task, tErr := d.GetTask(tte.TaskId)
		if tErr != nil {
			log.Error().Err(dErr).Msgf("Could not get task %s from DAG: %s", tte.TaskId, tte.DagId)
			break
		}
		task.Execute()
		time.Sleep(3 * time.Second)
	}
}
