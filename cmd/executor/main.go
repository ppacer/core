package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dskrzypiec/scheduler/cmd/executor/sched_client"
	"github.com/dskrzypiec/scheduler/src/dag"

	_ "github.com/dskrzypiec/scheduler/src/user"
)

func main() {
	cfg := ParseConfig()
	cfg.setupLogger()

	const dagId = "hello_dag"
	helloDag, err := dag.Get(dag.Id(dagId))
	if err != nil {
		slog.Error("Could no get DAG hello_dag from registry", "err", err)
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
			slog.Error("GetTask error", "err", err)
			break
		}
		fmt.Printf("Executing %s.%s.Execute()...\n", tte.DagId, tte.TaskId)
		d, dErr := dag.Get(dag.Id(tte.DagId))
		if dErr != nil {
			slog.Error("Could not get DAG from registry", "dagId", tte.DagId)
			break
		}
		task, tErr := d.GetTask(tte.TaskId)
		if tErr != nil {
			slog.Error("Could not get task from DAG", "dagId", tte.DagId,
				"taskId", tte.TaskId)
			break
		}
		task.Execute()
		time.Sleep(3 * time.Second)
	}
}
