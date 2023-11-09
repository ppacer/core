package db

import (
	"context"
	"database/sql"
	"sync"

	_ "modernc.org/sqlite"
)

type SqliteDB struct {
	sync.RWMutex
	dbConn     *sql.DB
	dbFilePath string
}

func (s *SqliteDB) Begin() (*sql.Tx, error) {
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
