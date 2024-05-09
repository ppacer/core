package db

import (
	"os"
	"testing"
)

func newClientForTesting(t *testing.T) (*Client, error) {
	t.Helper()
	usePostgrs, connStr, dbConn := usePostgres(t)
	useSqliteInMemory := os.Getenv("USE_SQLITE_MEM")
	if usePostgrs {
		client, err := NewPostgresClient(dbConn, connStr, nil)
		if err != nil {
			t.Errorf("Cannot create new Client for Postgres database: %s",
				err.Error())
		}
		t.Log("USING POSTGRES!")
		return client, nil
	}
	if useSqliteInMemory == "1" {
		return NewSqliteInMemoryClient(nil)
	}
	t.Log("USING SQLite in tmp file!")
	return NewSqliteTmpClient(nil)
}
