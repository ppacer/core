// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

func TestNewPostgresClient(t *testing.T) {
	use, connStr, dbConn := usePostgres(t)
	if !use {
		t.Skip("Postgres credentials are not set in env variables.")
	}

	c, err := NewPostgresClient(dbConn, connStr, nil)
	if err != nil {
		t.Errorf("Cannot create new Client for Postgres database: %s",
			err.Error())
	}
	if len(c.dbConn.DataSource()) == 0 {
		t.Error("Expected non-empty data source for postgres client")
	}

	for _, tableName := range TableNames {
		cnt := c.Count(tableName)
		if cnt == -1 {
			t.Errorf("Expected %s table to exist", tableName)
		}
	}

}

func usePostgres(t *testing.T) (bool, string, *sql.DB) {
	t.Helper()
	if os.Getenv("USE_POSTGRES") != "1" {
		return false, "", nil
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		return false, "", nil
	}
	dbName := os.Getenv("POSTGRES_DBNAME")
	if dbName == "" {
		return false, "", nil
	}

	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Cannot connect to Postgres (%s): %s",
			connStr, err.Error())
	}
	return true, connStr, db
}

func getEnvOrDefault(envKey, defaultValue string) string {
	val := defaultValue
	valEnv := os.Getenv(envKey)
	if valEnv != "" {
		val = valEnv
	}
	return val
}
