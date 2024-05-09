// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
)

// Produces new Client based on given connection to PostgreSQL database. If
// given database doesn't contain required ppacer schema, it will be created on
// the client initialization.
func NewPostgresClient(dbConn *sql.DB, dbName string, logger *slog.Logger) (*Client, error) {
	return setupPostgresSchemaIfNotExists(
		dbConn, dbName, logger, setupPostgresSchema,
	)
}

// Produces new Client for logs based on given connection to PostgreSQL
// database. If given database doesn't contain required logs schema, it will
// be created on the client initialization.
func NewPostgresClientForLogs(dbConn *sql.DB, dbName string, logger *slog.Logger) (*Client, error) {
	return setupPostgresSchemaIfNotExists(
		dbConn, dbName, logger, setupPostgresSchemaForLogs,
	)
}

func setupPostgresSchemaIfNotExists(
	dbConn *sql.DB, dbName string, logger *slog.Logger,
	setupSchemaFunc func(*sql.DB) error,
) (*Client, error) {
	schemaIsAlreadySet, schemaCheckErr := isPostgresSchemaSet(dbConn, TableNames)
	if schemaCheckErr != nil {
		return nil, schemaCheckErr
	}
	if !schemaIsAlreadySet {
		schemaSetErr := setupSchemaFunc(dbConn)
		if schemaSetErr != nil {
			return nil, schemaSetErr
		}
	}
	if logger == nil {
		opts := slog.HandlerOptions{Level: slog.LevelWarn}
		logger = slog.New(slog.NewTextHandler(os.Stdout, &opts))
	}
	postgresDB := PostgresDB{dbConn: dbConn, dbName: dbName}
	return &Client{
		dbConn:   &postgresDB,
		dbDriver: Postgres,
		logger:   logger,
	}, nil
}

func isPostgresSchemaSet(dbConn *sql.DB, expectedTableNames []string) (bool, error) {
	query := "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
	rows, qErr := dbConn.Query(query)
	if qErr != nil {
		return false, qErr
	}
	defer rows.Close()

	existingTables := make(map[string]struct{})
	for rows.Next() {
		var tablename string
		if err := rows.Scan(&tablename); err != nil {
			return false, err
		}
		existingTables[tablename] = struct{}{}
	}

	for _, expTableName := range expectedTableNames {
		if _, ok := existingTables[expTableName]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func setupPostgresSchema(db *sql.DB) error {
	schemaStmts, err := SchemaStatements("postgres")
	if err != nil {
		return err
	}
	return execSqlStatements(db, schemaStmts)
}

func setupPostgresSchemaForLogs(db *sql.DB) error {
	schemaStmts, err := SchemaStatementsForLogs("postgres")
	if err != nil {
		return err
	}
	return execSqlStatements(db, schemaStmts)
}

// PostgresDB represents Client for PostgreSQL database.
type PostgresDB struct {
	dbConn *sql.DB
	dbName string
}

func (s *PostgresDB) Begin() (*sql.Tx, error) {
	return s.dbConn.Begin()
}

func (s *PostgresDB) Exec(query string, args ...any) (sql.Result, error) {
	return s.dbConn.Exec(query, args...)
}

func (s *PostgresDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.dbConn.ExecContext(ctx, query, args...)
}

func (s *PostgresDB) Close() error {
	return s.dbConn.Close()
}

func (s *PostgresDB) DataSource() string {
	return s.dbName
}

func (s *PostgresDB) Query(query string, args ...any) (*sql.Rows, error) {
	return s.dbConn.Query(query, args...)
}

func (s *PostgresDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.dbConn.QueryContext(ctx, query, args...)
}

func (s *PostgresDB) QueryRow(query string, args ...any) *sql.Row {
	return s.dbConn.QueryRow(query, args...)
}

func (s *PostgresDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.dbConn.QueryRowContext(ctx, query, args...)
}
