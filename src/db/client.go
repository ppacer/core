package db

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

// Clinet represents the main database client.
type Client struct {
	dbConn *sql.DB
}

// Produces new Client based on given connection string to SQLite database.
func NewClient(connString string) (*Client, error) {
	db, dbErr := sql.Open("sqlite", connString)
	if dbErr != nil {
		log.Error().Err(dbErr).Msgf("Couldn't connect to SQLite [%s]", connString)
		return nil, fmt.Errorf("cannot connect to SQLite DB: %w", dbErr)
	}
	return &Client{db}, nil
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
