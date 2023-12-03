package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

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

// Clinet represents the main database client.
type Client struct {
	dbConn DB
}

// Produces new Client based on given connection string to SQLite database.
func NewClient(connString string) (*Client, error) {
	db, dbErr := sql.Open("sqlite", connString)
	if dbErr != nil {
		slog.Error("Could not connect to SQLite", "connString", connString,
			"err", dbErr)
		return nil, fmt.Errorf("cannot connect to SQLite DB: %w", dbErr)
	}
	sqliteDB := SqliteDB{dbConn: db}
	return &Client{&sqliteDB}, nil
}

// Produces new Client using in-memory SQLite database with schema created based on given script.
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
	return &Client{&sqliteDB}, nil
}

// Produces new Client using SQLite database created as temp file. It's mainly
// for testing and ad-hocs.
func NewSqliteTmpClient(schemaScriptPath string) (*Client, error) {
	schema, err := os.ReadFile(schemaScriptPath)
	if err != nil {
		return nil, err
	}

	// Create a temporary file to hold the SQLite database
	tmpFile, err := os.CreateTemp("", "sqlite-")
	if err != nil {
		return nil, err
	}
	tmpFilePath := tmpFile.Name()
	tmpFile.Close()

	// Connect to the SQLite database using the temporary file path
	db, err := sql.Open("sqlite", tmpFilePath)
	if err != nil {
		os.Remove(tmpFilePath)
		return nil, err
	}
	execQueries := strings.Split(string(schema), ";")
	for _, query := range execQueries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		_, err = db.Exec(query)
		if err != nil {
			db.Close()
			os.Remove(tmpFilePath)
			return nil, err
		}
	}
	sqliteDB := SqliteDB{dbConn: db, dbFilePath: tmpFilePath}
	return &Client{&sqliteDB}, nil
}

// CleanUpSqliteTmp deletes SQLite database source file if all tests in the
// scope passed. In at least one test failed, database will not be deleted, to
// enable futher debugging. Even though this function takes generic *Client,
// it's mainly meant for SQLite-based database clients which are used in
// testing.
func CleanUpSqliteTmp(c *Client, t *testing.T) {
	if closeErr := c.dbConn.Close(); closeErr != nil {
		t.Errorf("Error while closing connection to DB: %s", closeErr.Error())
	}
	if t.Failed() {
		t.Logf("Database was not deleted. Please check: sqlite3 %s",
			c.dbConn.DataSource())
		return
	}
	// tests passed, we can proceed to remove DB file
	if err := os.Remove(c.dbConn.DataSource()); err != nil {
		t.Errorf("Cannot remove database source file %s: %s",
			c.dbConn.DataSource(), err.Error())
	}
}
