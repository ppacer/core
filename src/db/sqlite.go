package db

import (
	"context"
	"database/sql"
	"sync"

	_ "modernc.org/sqlite"
)

type SqliteDB struct {
	sync.Mutex
	dbConn *sql.DB
}

func (s *SqliteDB) Begin() (*sql.Tx, error) {
	return s.dbConn.Begin()
}

func (s *SqliteDB) Exec(query string, args ...any) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.Exec(query, args...)
}

func (s *SqliteDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	s.Lock()
	defer s.Unlock()
	return s.dbConn.ExecContext(ctx, query, args...)
}

func (s *SqliteDB) Query(query string, args ...any) (*sql.Rows, error) {
	return s.dbConn.Query(query, args...)
}

func (s *SqliteDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.dbConn.QueryContext(ctx, query, args...)
}

func (s *SqliteDB) QueryRow(query string, args ...any) *sql.Row {
	return s.dbConn.QueryRow(query, args...)
}

func (s *SqliteDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.dbConn.QueryRowContext(ctx, query, args...)
}
