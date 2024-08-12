# Not released changes

# [v0.0.4] - 2024-08-12

- Change DRTBase.AtTime from time.Time to string. Serialized time.Time can
differ from the original value on some platform (e.g. Linux, x86).


# [v0.0.3] - 2024-08-06

- Introduce `/ui/dagrun/*` endpoints in Scheduler HTTP server, to hydrate the
    main UI page.
- Fix counting goroutines in TaskScheduler.
- Introduce `scheduler.API`, so ppacer UI could mock it.


# [v0.0.2] - 2024-08-01

- Introduced core.DefaultStarted, to reduce boilerplate in most simple examples
and tests.


# [v0.0.1] - 2024-07-24

## Backend - new features


### Scheduler

- Scheduler can schedule multiple (10000 cap by default) concurrent DAG runs
  and its tasks at the same time.
- Scheduler synchronise new DAGs and changes in existing DAGs between its
  restarts/deployments.
- Scheduler synchronise, on the start up, unfinished DAG runs and their task
  execution statuses, to continue scheduling remaining part of the process.
- Sending external notifications on DAG run task retries or failures is
  supported.
- All information on DAGs, DAG tasks, DAG runs and tasks execution are kept in
  SQLite database by default.
- Timezone, for all timestamps created by ppacer, can be set in Scheduler
  configuration. By default it's local timezone.


### DAGs and Tasks

- DAGs can be defined via fluent API in `dag` package.
- DAGs schedules are defined by generic interface.
- Cron schedule and fixed-interval schedule are included in
  `ppacer/core/dag/schedule` package.
- Task configuration (`dag.Node.Config`) supports:
    - Number of task retries.
    - Task execution timeout.
    - Delayed retries.
    - Sending alerts on task execution failure.
    - Sending alerts on task execution retry.
    - Setting custom alert message templates.
    - Setting custom notifier on a Task level.
- Source code of Task Execute methods are parsed and stored in the database.


### Executor

- Executor supports upper limit for number of task executing in separate
  goroutines (default is 1000).
- Executor respects configured task's timeout.
- Executor supports custom strategies for polling the Scheduler.
- Executor and Scheduler might be included in the same program (in two separate
  goroutines).
- Task logs (logs produced by Tasks execution body) are stored in a separate
  SQLite database.


### Database

- Package `db` provides abstraction over standard `sql.DB`, to support any
  database which implements `database/sql` driver.
- Currently SQLite is supported and used as the main database both for the
  Scheduler and task logs.
- SQLite database in temp files are used for unit and end-to-end tests.


