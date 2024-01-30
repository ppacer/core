// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"database/sql"
	"testing"
)

func TestSqliteSchema(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Errorf("Cannot create in-memory SQLite database: %s:", err.Error())
	}
	schemaStmts, schemaErr := SchemaStatements("sqlite")
	if schemaErr != nil {
		t.Errorf("Cannot get schema for sqlite: %s", schemaErr.Error())
	}

	for _, stmt := range schemaStmts {
		_, err = db.Exec(stmt)
		if err != nil {
			t.Errorf("Error while executing [%s] query: %s", stmt, err.Error())
		}
	}
}
