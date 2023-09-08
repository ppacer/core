package db

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const LOG_PREFIX = "db"
const InsertTsFormat = "2006-01-02T15:04:05.999Z07:00"

// Count returns count of rows for given table. If case of errors -1 is returned and error is logged.
func (c *Client) Count(table string) int {
	start := time.Now()
	log.Info().Str("table", table).Msgf("[%s] Start COUNT query.", LOG_PREFIX)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	row := c.dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msgf("[%s] Cannot execute COUNT(*)", LOG_PREFIX)
		return -1
	}

	log.Info().Str("table", table).Dur("durationMs", time.Since(start)).Msgf("[%s] Finished COUNT(*) query", LOG_PREFIX)
	return count
}

// CountWhere returns count of rows for given table filtered by given where condition. If case of errors -1 is returned
// and error is logged.
func (c *Client) CountWhere(table, where string) int {
	start := time.Now()
	log.Info().Str("table", table).Str("where", where).Msgf("[%s] Start COUNT query.", LOG_PREFIX)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)
	row := c.dbConn.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Error().Err(err).Str("table", table).Str("where", where).Msgf("[%s] Cannot execute COUNT(*)", LOG_PREFIX)
		return -1
	}

	log.Info().Str("table", table).Str("where", where).Dur("durationMs", time.Since(start)).
		Msgf("[%s] Finished COUNT(*) query", LOG_PREFIX)
	return count
}
