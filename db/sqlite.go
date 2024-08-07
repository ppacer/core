// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// Produces new Client based on given connection string to SQLite database. If
// database file does not exist in given location, then empty SQLite database
// with setup schema will be created.
func NewSqliteClient(dbFilePath string, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = defaultLogger()
	}
	sqliteDb, err := newSqliteClientForSchema(dbFilePath, setupSqliteSchema)
	if err != nil {
		return nil, err
	}
	client := &Client{
		dbConn: sqliteDb,
		logger: logger,
	}
	return client, nil
}

// Produces new Client based on SQLite in-memory database. It's not persisted
// anywhere else. Useful usually for tests and small examples.
func NewSqliteInMemoryClient(logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = defaultLogger()
	}
	return newSqliteInMemoryClientForSchema(logger, setupSqliteSchema)
}

// Produces new Client for logs based on given connection string to SQLite
// database. If database file does not exist in given location, then empty
// SQLite database with setup schema will be created.
func NewSqliteClientForLogs(dbFilePath string, logger *slog.Logger) (*LogsClient, error) {
	if logger == nil {
		logger = defaultLogger()
	}
	sqliteDb, err := newSqliteClientForSchema(dbFilePath, setupSqliteSchemaForLogs)
	if err != nil {
		return nil, err
	}
	client := &LogsClient{
		dbConn: sqliteDb,
		logger: logger,
	}
	return client, nil
}

func newSqliteClientForSchema(
	dbFilePath string, setupSchemaFunc func(*sql.DB) error,
) (*SqliteDB, error) {
	dbFilePathAbs, absErr := filepath.Abs(dbFilePath)
	if absErr != nil {
		return nil, fmt.Errorf("cannot get absolute path of database file %s: %w",
			dbFilePath, absErr)
	}
	newDbCreated, dbFileErr := createSqliteDbIfNotExist(dbFilePathAbs)
	if dbFileErr != nil {
		return nil, fmt.Errorf("cannot create new empty SQLite database: %w",
			dbFileErr)
	}
	connString := sqliteConnString(dbFilePathAbs)
	db, dbErr := sql.Open("sqlite", connString)
	if dbErr != nil {
		return nil, fmt.Errorf("cannot connect to SQLite DB (%s): %w",
			connString, dbErr)
	}
	if newDbCreated {
		schemaErr := setupSchemaFunc(db)
		if schemaErr != nil {
			db.Close()
			return nil, fmt.Errorf("cannot setup SQLite schema for %s: %w",
				connString, schemaErr)
		}
	}
	return &SqliteDB{dbConn: db, dbFilePath: dbFilePathAbs}, nil
}

func newSqliteInMemoryClientForSchema(
	logger *slog.Logger, setupSchemaFunc func(*sql.DB) error,
) (*Client, error) {
	db, dbErr := sql.Open("sqlite", ":memory:")
	if dbErr != nil {
		return nil, fmt.Errorf("cannot connect to SQLite DB (in-memory): %w",
			dbErr)
	}
	schemaErr := setupSchemaFunc(db)
	if schemaErr != nil {
		db.Close()
		return nil, fmt.Errorf("cannot setup SQLite schema for in-memory: %w",
			schemaErr)
	}
	if logger == nil {
		logger = defaultLogger()
	}
	sqliteDB := SqliteDB{dbConn: db, dbFilePath: ":memory:"}
	return &Client{&sqliteDB, logger}, nil
}

// Produces new Client using SQLite database created as temp file. It's mainly
// for testing and ad-hocs.
func NewSqliteTmpClient(logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = defaultLogger()
	}
	sqliteDb, err := newSqliteTmpClientForSchema(
		"sqlite-", setupSqliteSchema,
	)
	if err != nil {
		return nil, err
	}
	client := &Client{
		dbConn: sqliteDb,
		logger: logger,
	}
	return client, nil
}

// Produces new Client for logs using SQLite database created as temp file.
// It's mainly for testing and ad-hocs.
func NewSqliteTmpClientForLogs(logger *slog.Logger) (*LogsClient, error) {
	if logger == nil {
		logger = defaultLogger()
	}
	sqliteDb, err := newSqliteTmpClientForSchema(
		"sqlitelogs-", setupSqliteSchemaForLogs,
	)
	if err != nil {
		return nil, err
	}
	client := &LogsClient{
		dbConn: sqliteDb,
		logger: logger,
	}
	return client, nil
}

func newSqliteTmpClientForSchema(
	prefix string, setupSchemaFunc func(*sql.DB) error,
) (*SqliteDB, error) {
	tmpFile, err := os.CreateTemp("", prefix)
	if err != nil {
		return nil, err
	}
	tmpFilePath := tmpFile.Name()
	tmpFile.Close()

	// Connect to the SQLite database using the temporary file path
	db, err := sql.Open("sqlite", sqliteConnString(tmpFilePath))
	if err != nil {
		os.Remove(tmpFilePath)
		return nil, err
	}

	schemaErr := setupSchemaFunc(db)
	if schemaErr != nil {
		db.Close()
		os.Remove(tmpFilePath)
		return nil, fmt.Errorf("cannot setup SQLite schema: %w", schemaErr)
	}
	return &SqliteDB{dbConn: db, dbFilePath: tmpFilePath}, nil
}

func sqliteConnString(dbFilePath string) string {
	// TODO: probably read from the config not only database file path but also
	// additional arguments also.
	options := "cache=shared&mode=rwc&_journal_mode=WAL"
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("%s?%s", dbFilePath, options)
	}
	return fmt.Sprintf("file://%s?%s", dbFilePath, options)
}

func setupSqliteSchema(db *sql.DB) error {
	schemaStmts, err := SchemaStatements("sqlite")
	if err != nil {
		return err
	}
	return execSqlStatements(db, schemaStmts)
}

func setupSqliteSchemaForLogs(db *sql.DB) error {
	schemaStmts, err := SchemaStatementsForLogs("sqlite")
	if err != nil {
		return err
	}
	return execSqlStatements(db, schemaStmts)
}

func execSqlStatements(db *sql.DB, stmts []string) error {
	for _, query := range stmts {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func createSqliteDbIfNotExist(dbFilePath string) (bool, error) {
	if _, err := os.Stat(dbFilePath); os.IsNotExist(err) {
		dirErr := os.MkdirAll(filepath.Dir(dbFilePath), os.ModePerm)
		if dirErr != nil {
			return false, dirErr
		}

		file, fErr := os.Create(dbFilePath)
		if fErr != nil {
			return false, fErr
		}
		file.Close()
		return true, nil
	}

	return false, nil
}

type SqliteDB struct {
	sync.RWMutex
	dbConn     *sql.DB
	dbFilePath string
}

func (s *SqliteDB) Begin() (*sql.Tx, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Begin()
}

func (s *SqliteDB) Exec(query string, args ...any) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Exec(query, args...)
}

func (s *SqliteDB) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.ExecContext(ctx, query, args...)
}

func (s *SqliteDB) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Close()
}

func (s *SqliteDB) DataSource() string {
	return s.dbFilePath
}

func (s *SqliteDB) Query(query string, args ...any) (*sql.Rows, error) {
	s.RLock()
	defer s.RUnlock()
	return s.dbConn.Query(query, args...)
}

func (s *SqliteDB) QueryContext(
	ctx context.Context, query string, args ...any,
) (*sql.Rows, error) {
	s.RLock()
	defer s.RUnlock()
	return s.dbConn.QueryContext(ctx, query, args...)
}

func (s *SqliteDB) QueryRow(query string, args ...any) *sql.Row {
	s.RLock()
	defer s.RUnlock()
	return s.dbConn.QueryRow(query, args...)
}

func (s *SqliteDB) QueryRowContext(
	ctx context.Context, query string, args ...any,
) *sql.Row {
	s.RLock()
	defer s.RUnlock()
	return s.dbConn.QueryRowContext(ctx, query, args...)
}

// SQLite database where data is stored in the memory rather than in a file on
// a disk. It needs additional level of isolation for concurrent access.
type SqliteDBInMemory struct {
	sync.Mutex
	dbConn *sql.DB
}

func (s *SqliteDBInMemory) Begin() (*sql.Tx, error) {
	return s.dbConn.Begin()
}

func (s *SqliteDBInMemory) Exec(query string, args ...any) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Exec(query, args...)
}

func (s *SqliteDBInMemory) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.ExecContext(ctx, query, args...)
}

func (s *SqliteDBInMemory) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Close()
}

func (s *SqliteDBInMemory) DataSource() string {
	return "IN_MEMORY"
}

func (s *SqliteDBInMemory) Query(query string, args ...any) (*sql.Rows, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Query(query, args...)
}

func (s *SqliteDBInMemory) QueryContext(
	ctx context.Context, query string, args ...any,
) (*sql.Rows, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.QueryContext(ctx, query, args...)
}

func (s *SqliteDBInMemory) QueryRow(query string, args ...any) *sql.Row {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.QueryRow(query, args...)
}

func (s *SqliteDBInMemory) QueryRowContext(
	ctx context.Context, query string, args ...any,
) *sql.Row {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.QueryRowContext(ctx, query, args...)
}
