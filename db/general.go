// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// Scannable interface is mainly to define parsers both for *sql.Row and
// *sql.Rows types.
type Scannable interface {
	Scan(...any) error
}

// Count returns count of rows for given table. If case of errors -1 is
// returned and error is logged.
func (c *Client) Count(table string) int {
	return count(c.dbConn, c.logger, table)
}

// Count returns count of rows for given table. If case of errors -1 is
// returned and error is logged.
func (lc *LogsClient) Count(table string) int {
	return count(lc.dbConn, lc.logger, table)
}

func count(dbConn DB, logger *slog.Logger, table string) int {
	start := time.Now()
	logger.Debug("Start COUNT query", "table", table)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	row := dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		logger.Error("Cannot execute COUNT(*)", "table", table, "err", err)
		return -1
	}
	logger.Debug("Finished COUNT(*) query", "table", table, "duration",
		time.Since(start))
	return count
}

// CountWhere returns count of rows for given table filtered by given where
// condition. If case of errors -1 is returned and error is logged.
func (c *Client) CountWhere(table, where string) int {
	start := time.Now()
	c.logger.Debug("Start COUNT query", "table", table, "where", where)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)
	row := c.dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		c.logger.Error("Cannot execute COUNT(*)", "table", table, "where", where)
		return -1
	}
	c.logger.Debug("Finished COUNT(*) query", "table", table, "where", where,
		"duration", time.Since(start))
	return count
}

func (c *Client) singleInt64(ctx context.Context, cntQuery string, args ...any) (int64, error) {
	start := time.Now()
	c.logger.Debug("Start single value query", "query", cntQuery, "args", args)

	row := c.dbConn.QueryRowContext(ctx, cntQuery, args...)
	var count int64
	err := row.Scan(&count)
	if err != nil {
		c.logger.Error("Cannot execute count query", "query", cntQuery, "args",
			args)
		return -1, err
	}
	c.logger.Debug("Finished single value query", "query", cntQuery, "args",
		args, "duration", time.Since(start))
	return count, nil
}

// Function groupBy2 executes usual SQL aggregation in form of:
//
// # SELECT Col1, F(Col2) FROM table GROUP BY Col1
//
// Query might be more complex, the only assumption is for it, to return two
// columns of type K and V.
func groupBy2[K comparable, V any](
	ctx context.Context, dbConn DB, logger *slog.Logger, query string, args ...any,
) (map[K]V, error) {
	start := time.Now()
	logger.Debug("Start groupBy2 query", "query", query, "args", args)
	result := make(map[K]V)

	rows, qErr := dbConn.QueryContext(ctx, query, args...)
	if qErr != nil {
		logger.Error("Cannot execute groupBy2 query", "query", query, "args",
			args, "err", qErr.Error())
	}
	var key K
	var value V

	for rows.Next() {
		select {
		case <-ctx.Done():
			// Handle context cancellation or deadline exceeded
			logger.Warn("Context done while processing rows", "err", ctx.Err())
			return nil, ctx.Err()
		default:
		}
		scanErr := rows.Scan(&key, &value)
		if scanErr != nil {
			logger.Error("Error while scanning groupBy2 query results", "err",
				scanErr.Error())
			return nil, scanErr
		}
		result[key] = value
	}

	logger.Debug("Finished groupBy2 query", "query", query, "args", args,
		"duration", time.Since(start))
	return result, nil
}

// ReadRow executes given query, which is expected to produce at most one row
// of outcome, and tries to cast it onto given type T.
func readRow[T any](
	ctx context.Context,
	dbConn DB,
	logger *slog.Logger,
	scanner func(Scannable) (T, error),
	query string, args ...any,
) (T, error) {
	start := time.Now()
	logger.Debug("Start processing single row query...", "query", query,
		"args", args)

	row := dbConn.QueryRowContext(ctx, query, args...)
	result, scanErr := scanner(row)
	if scanErr != nil {
		if scanErr != sql.ErrNoRows {
			logger.Error("Scanning SQL row failed", "err", scanErr.Error())
		}
		return result, scanErr
	}

	logger.Debug("Processing single row query finished", "query", query,
		"args", args, "duration", time.Since(start))
	return result, nil
}

func readRowsContext[T any](
	ctx context.Context,
	dbConn DB,
	logger *slog.Logger,
	scanner func(Scannable) (T, error),
	query string,
	args ...any,
) ([]T, error) {
	start := time.Now()
	logger.Debug("Start processing multiple rows query...", "query", query,
		"args", args)
	result := make([]T, 0, 100)
	rows, qErr := dbConn.QueryContext(ctx, query, args...)
	if qErr != nil {
		logger.Error("SQL query failed", "query", query, "args", args)
		return result, qErr
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			logger.Warn("Context done while rows", "query", query,
				"args", args, "err", ctx.Err())
			return result, ctx.Err()
		default:
		}
		obj, scanErr := scanner(rows)
		if scanErr != nil {
			if scanErr != sql.ErrNoRows {
				logger.Error("Scanning SQL row failed", "err", scanErr.Error())
			}
			return result, scanErr
		}
		result = append(result, obj)
	}
	logger.Debug("Processing multiple rows query finished", "query", query,
		"args", args, "duration", time.Since(start))
	return result, nil
}
