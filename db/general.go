// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Count returns count of rows for given table. If case of errors -1 is
// returned and error is logged.
func (c *Client) Count(table string) int {
	start := time.Now()
	slog.Debug("Start COUNT query", "table", table)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	row := c.dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		slog.Error("Cannot execute COUNT(*)", "table", table, "err", err)
		return -1
	}
	slog.Debug("Finished COUNT(*) query", "table", table, "duration",
		time.Since(start))
	return count
}

// CountWhere returns count of rows for given table filtered by given where
// condition. If case of errors -1 is returned and error is logged.
func (c *Client) CountWhere(table, where string) int {
	start := time.Now()
	slog.Debug("Start COUNT query", "table", table, "where", where)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)
	row := c.dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		slog.Error("Cannot execute COUNT(*)", "table", table, "where", where)
		return -1
	}
	slog.Debug("Finished COUNT(*) query", "table", table, "where", where,
		"duration", time.Since(start))
	return count
}

func (c *Client) singleInt64(ctx context.Context, cntQuery string, args ...any) (int64, error) {
	start := time.Now()
	slog.Debug("Start single value query", "query", cntQuery, "args", args)

	row := c.dbConn.QueryRowContext(ctx, cntQuery, args...)
	var count int64
	err := row.Scan(&count)
	if err != nil {
		slog.Error("Cannot execute count query", "query", cntQuery, "args", args)
		return -1, err
	}
	slog.Debug("Finished single value query", "query", cntQuery, "args", args,
		"duration", time.Since(start))
	return count, nil
}
