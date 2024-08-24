// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"fmt"
)

// TableNames is a list of ppacer database table names.
var TableNames []string = []string{
	"dags",
	"dagtasks",
	"dagruns",
	"dagruntasks",
	"schedules",
}

// SchemaStatements returns a list of SQL statements that setups new instance
// of scheduler internal database. It can differ a little bit between SQL
// databases, so exact list of statements are prepared based on given database
// driver name. If given database driver is not supported, then non-nil error
// is returned.
func SchemaStatements(dbDriver string) ([]string, error) {
	if dbDriver == "sqlite" || dbDriver == "sqlite3" {
		return []string{
			sqliteSetupWAL(),
			sqliteCreateDagsTable(),
			sqliteCreateDagtasksTable(),
			sqliteCreateDagrunsTable(),
			sqliteCreateDagruntasksTable(),
			sqliteCreateSchedulesTable(),
		}, nil
	}

	return []string{}, fmt.Errorf("there is no schema for %s driver defined",
		dbDriver)
}

func sqliteSetupWAL() string {
	return "PRAGMA journal_mode = WAL;"
}

func sqliteCreateDagsTable() string {
	return `
-- Table dags stores DAGs and its metadata. Information about DAG tasks are
-- stored in dagtasks table.
CREATE TABLE IF NOT EXISTS dags (
    DagId TEXT NOT NULL,            -- DAG ID
    StartTs TEXT NULL,              -- DAG start timestamp
    Schedule TEXT NULL,             -- DAG schedule
    CreateTs TEXT NOT NULL,         -- Timestamp when DAG was initially inserted
    LatestUpdateTs TEXT NULL,       -- Timestamp of the DAG latest update
    CreateVersion TEXT NOT NULL,    -- Verion when DAG was innitially inserted
    LatestUpdateVersion TEXT NULL,  -- Version of DAG latest update
    HashDagMeta TEXT NOT NULL,      -- SHA256 hash of DAG attributes + StartTs + Schedule
    HashTasks TEXT NOT NULL,        -- SHA256 hash of DAG tasks
    Attributes TEXT NOT NULL,       -- DAG attributes like tags
    -- TODO: probably many more, but sometime later

    PRIMARY KEY (DagId)
);
`
}

func sqliteCreateDagtasksTable() string {
	return `
-- Table dagtasks represents tasks in dags. It contains history of changes.
-- Current state of all DAGs and its tasks can -- be determined by using
-- IsCurrent=1 condition.
CREATE TABLE IF NOT EXISTS dagtasks (
    DagId TEXT NOT NULL,            -- DAG ID
    TaskId TEXT NOT NULL,           -- Task ID
    IsCurrent INT NOT NULL,         -- Flag if pair (DagId, TaskId) represents the current version
    InsertTs TEXT NOT NULL,         -- Insert timestamp in %Y-%m-%D %H:%M:%S format
    PosDepth INT NOT NULL,          -- Task depth in the graph
    PosWidth INT NOT NULL,          -- Task width in the graph
    Version TEXT NOT NULL,          -- Scheduler Version
    TaskTypeName TEXT NOT NULL,     -- Go type name which implements this task
    TaskConfig TEXT NOT NULL,       -- Task configuration in form of JSON
    TaskBodyHash TEXT NOT NULL,     -- Task Execute() method body source code hash
    TaskBodySource TEXT NOT NULL,   -- Task Execute() method body source code as text

    PRIMARY KEY (DagId, TaskId, IsCurrent)
);
`
}

func sqliteCreateDagrunsTable() string {
	return `
-- Table dagruns stores DAG runs information. Runs might be both scheduled for
-- manually triggered.
CREATE TABLE IF NOT EXISTS dagruns (
    RunId INTEGER PRIMARY KEY,      -- Run ID - auto increments
    DagId TEXT NOT NULL,            -- DAG ID
    ExecTs TEXT NOT NULL,           -- Execution timestamp
    InsertTs TEXT NOT NULL,         -- Row insertion timestamp
    Status TEXT NOT NULL,           -- DAG run status
    StatusUpdateTs TEXT NOT NULL,   -- Status update timestamp (on first insert it's the same as InsertTs)
    Version TEXT NOT NULL           -- Scheduler Version
);
`
}

func sqliteCreateDagruntasksTable() string {
	return `
-- Table dagruntasks stores information about tasks state of DAG runs.
CREATE TABLE IF NOT EXISTS dagruntasks (
    DagId TEXT NOT NULL,            -- DAG ID
    ExecTs TEXT NOT NULL,           -- Execution timestamp
    TaskId TEXT NOT NULL,           -- Task ID
    Retry INT NOT NULL,             -- Identifier for task retry. For initial run it's 0.
    InsertTs TEXT NOT NULL,         -- Insert timestamp
    Status TEXT NOT NULL,           -- DAG task execution status
    StatusUpdateTs TEXT NOT NULL,   -- Status update timestamp (on first insert it's the same as InsertTs)
    Version TEXT NOT NULL,          -- Scheduler version

    PRIMARY KEY (DagId, ExecTs, TaskId, Retry)
);
`
}

func sqliteCreateSchedulesTable() string {
	return `
-- Table schedules stores information about DAG schedules, including regular
-- planned schedules, manual triggers, missed schedules etc.
CREATE TABLE IF NOT EXISTS schedules (
    DagId TEXT NOT NULL,           -- DAG ID
    InsertTs TEXT NOT NULL,        -- Insert timestamp
    Event TEXT NOT NULL,           -- Schedule related event (TODO)
    ScheduleTs TEXT NULL,          -- Schedule timestamp for the DAG
    NextScheduleTs TEXT NOT NULL,  -- Next planned Schedule timestamp

    PRIMARY KEY (DagId, InsertTs, Event)
);
`
}
