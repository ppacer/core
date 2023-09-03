package db

import (
	"database/sql"
	"fmt"

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
