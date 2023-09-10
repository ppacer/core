package db

import (
	"database/sql"
	"os"
	"path"
	"strings"

	_ "modernc.org/sqlite"
)

// Creates in-memory Sqlite3 database with defined schema.
func emptyDbWithSchema() (*Client, error) {
	// Read SQL schema from the file
	schemaPath := path.Join("..", "..", "schema.sql")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	// Create SQLite in-memory database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	// Execute schema query
	execQueries := strings.Split(string(schema), ";")
	for _, query := range execQueries {
		_, err = db.Exec(query)
		if err != nil {
			return nil, err
		}
	}
	return &Client{db}, nil
}
