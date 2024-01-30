// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
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
