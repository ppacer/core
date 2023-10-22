package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

type DB interface {
	Begin() (*sql.Tx, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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
		log.Error().Err(dbErr).Msgf("Couldn't connect to SQLite [%s]", connString)
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
	return &Client{db}, nil
}
