// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import "fmt"

// Like SchemaStatements but for database for logs.
func SchemaStatementsForLogs(dbDriver string) ([]string, error) {
	if dbDriver == "sqlite" || dbDriver == "sqlite3" {
		return []string{
			sqliteCreateTaskLogsTable(),
		}, nil
	}
	return []string{}, fmt.Errorf("there is no schema for %s driver defined",
		dbDriver)
}

func sqliteCreateTaskLogsTable() string {
	return `
-- Table logs stores DAG run task logs.
CREATE TABLE IF NOT EXISTS tasklogs (
	Date TEXT NOT NULL,     -- DAG run date (just date, for indexing)
	DagId TEXT NOT NULL,    -- DAG ID
	ExecTs TEXT NOT NULL,   -- DAG run execution timestamp
	TaskId TEXT NOT NULL,   -- Task ID
	InsertTs TEXT NOT NULL, -- Row insertion timestamp
	Message TEXT NULL,      -- Log message

	PRIMARY KEY (Date DESC, DagId, ExecTs DESC, TaskId)
);
`
}
