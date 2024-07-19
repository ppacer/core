# ppacer

[Ppacer](https://ppacer.org) is a DAG scheduler aiming to provide high
reliability, high performance and minimal resource overhead.

This repository contains ppacer core Go packages.


## Getting started

If you would like to run ppacer "hello world" example and you do have Go
compiler in version `>= 1.22`, please follow this guide
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




