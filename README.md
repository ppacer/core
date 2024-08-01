# ppacer

[![Go Report Card](https://goreportcard.com/badge/github.com/ppacer/core)](https://goreportcard.com/report/github.com/ppacer/core)

[Ppacer](https://ppacer.org) is a DAG scheduler aiming to provide high
reliability, high performance and minimal resource overhead.

This repository contains ppacer core Go packages.


## Getting started

If you would like to run ppacer "hello world" example and you do have Go
compiler in version `>= 1.22`, you can simply `go get` ppacer packages (in
existing Go module)


```bash
go get github.com/ppacer/core@latest
go get github.com/ppacer/tasks@latest
```

And then run the following program:


```go
package main

import (
    "context"
    "time"

    "github.com/ppacer/core"
    "github.com/ppacer/core/dag"
    "github.com/ppacer/core/dag/schedule"
    "github.com/ppacer/tasks"
)

func main() {
    const port = 9321
    ctx := context.Background()
    dags := dag.Registry{}
    dags.Add(printDAG("printing_dag"))
    core.DefaultStarted(ctx, dags, port)
}

func printDAG(dagId string) dag.Dag {
    //         t21
    //       /
    // start
    //       \
    //         t22 --> finish
    start := dag.NewNode(tasks.NewPrintTask("start", "hello"))
    t21 := dag.NewNode(tasks.NewPrintTask("t21", "foo"))
    t22 := dag.NewNode(tasks.NewPrintTask("t22", "bar"))
    finish := dag.NewNode(tasks.NewPrintTask("finish", "I'm done!"))

    start.Next(t21)
    start.Next(t22)
    t22.Next(finish)

    startTs := time.Date(2024, time.March, 11, 12, 0, 0, 0, time.Local)
    schedule := schedule.NewFixed(startTs, 10*time.Second)

    printDag := dag.New(dag.Id(dagId)).
        AddSchedule(&schedule).
        AddRoot(start).
        Done()
    return printDag
}
```

Detailed instructions and a bit more of explanations are presented here:
[ppacer/intro](https://ppacer.org/start/intro).


## Development

Developers who use Linux or MacOS can simply run `./build.sh`, to build
packages, run tests and benchmarks.

Default log severity level is set to `WARN`. In case you need more details, you
can use `PPACER_LOG_LEVEL` env variable, like in the following examples:

```bash
PPACER_LOG_LEVEL=INFO go test -v ./...
```

In case when you need to just debug a single test, you run command similar to
the following one:

```bash
PPACER_LOG_LEVEL=DEBUG go test -v ./db -run=TestReadDagRunTaskSingle
```

