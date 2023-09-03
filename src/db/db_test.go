package db

import (
	"database/sql"
	"io/ioutil"
	"path"

	_ "modernc.org/sqlite"
)

// Creates in-memory Sqlite3 database with defined schema.
func emptyDbWithSchema() (*Client, error) {
	// Read SQL schema from the file
	schemaPath := path.Join("..", "..", "schema.sql")
	schema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	// Create SQLite in-memory database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	// Execute schema query
	_, err = db.Exec(string(schema))
	if err != nil {
		return nil, err
	}

	return &Client{db}, nil
}
