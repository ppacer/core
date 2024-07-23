// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

/*
Package db contains all communication between ppacer and the database.

# Introduction

# Supported databases

  - SQLite - used as the default database. It's also used as in-memory database
    and database on /tmp files for unit and integration tests.
  - Postgres
  - ... (More in the future)
*/
package db

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// DB defines a set of operations required from a database. Most of methods are
// identical with standard `*sql.DB` type.
type DB interface {
	Begin() (*sql.Tx, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Close() error
	DataSource() string
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// DBClient defines abstraction class for database clients.
type DBClient interface {
	DBConn() DB
}

// Client represents the main database client.
type Client struct {
	dbConn DB
	logger *slog.Logger
}

// Client is a DBClient.
func (c *Client) DBConn() DB { return c.dbConn }

// LogsClient represents ppacer task logs database client.
type LogsClient struct {
	dbConn DB
	logger *slog.Logger
}

// LogsClient is a DBClient.
func (lc *LogsClient) DBConn() DB { return lc.dbConn }

// Produces new Client using in-memory SQLite database with schema created
// based on given script.
func NewInMemoryClient(schemaScriptPath string) (*Client, error) {
	schema, err := os.ReadFile(schemaScriptPath)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	execQueries := strings.Split(string(schema), ";")
	for _, query := range execQueries {
		_, err = db.Exec(query)
		if err != nil {
			return nil, err
		}
	}
	sqliteDB := SqliteDB{dbConn: db}
	opts := slog.HandlerOptions{Level: slog.LevelInfo}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &opts))
	return &Client{&sqliteDB, logger}, nil
}

// CleanUpSqliteTmp deletes SQLite database source file if all tests in the
// scope passed. In at least one test failed, database will not be deleted, to
// enable futher debugging. Even though this function takes generic *Client,
// it's mainly meant for SQLite-based database clients which are used in
// testing.
func CleanUpSqliteTmp(c DBClient, t *testing.T) {
	if closeErr := c.DBConn().Close(); closeErr != nil {
		t.Errorf("Error while closing connection to DB: %s", closeErr.Error())
	}
	if t.Failed() {
		t.Logf("Database was not deleted. Please check: sqlite3 %s",
			c.DBConn().DataSource())
		return
	}
	// tests passed, we can proceed to remove DB file
	if err := os.Remove(c.DBConn().DataSource()); err != nil {
		t.Errorf("Cannot remove database source file %s: %s",
			c.DBConn().DataSource(), err.Error())
	}
}
