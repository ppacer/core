package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/ppacer/core/timeutils"
)

// Schedule represents a row in schedules table.
type Schedule struct {
	DagId          string
	InsertTs       string
	Event          string
	ScheduleTs     *string
	NextScheduleTs string
}

// ReadDagSchedules reads all schedule events for a given dag sorted from
// newest to oldest.
func (c *Client) ReadDagSchedules(ctx context.Context, dagId string) ([]Schedule, error) {
	start := time.Now()
	c.logger.Debug("Start reading dag schedule events", "dagId", dagId)
	dagSchedules := make([]Schedule, 0, 100)

	rows, qErr := c.dbConn.QueryContext(ctx, c.readDagSchedulesQuery(), dagId)
	if qErr != nil {
		c.logger.Error("Failed querying DAG schedules", "dagId", dagId, "err",
			qErr.Error())
	}
	for rows.Next() {
		select {
		case <-ctx.Done():
			c.logger.Warn("Context done while processing dag schedules", "err",
				ctx.Err())
			return nil, ctx.Err()
		default:
		}
		dagSchedule, scanErr := parseScheduleRow(rows)
		if scanErr != nil {
			c.logger.Error("Failed scanning a DAG Schedule record", "err", scanErr)
			continue
		}
		dagSchedules = append(dagSchedules, dagSchedule)
	}

	c.logger.Debug("Finished reading dag schedule events", "dagId", dagId,
		"duration", time.Since(start))
	return dagSchedules, nil
}

// InsertDagSchedule inserts new event regarding DAG schedule.
func (c *Client) InsertDagSchedule(
	ctx context.Context, dagId, event, nextSchedule string, schedule *string,
) error {
	start := time.Now()
	insertTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start inserting new DAG schedule event", "dagId", dagId,
		"event", event, "scheduleTs", schedule, "nextScheduleTs", nextSchedule)
	_, iErr := c.dbConn.ExecContext(
		ctx, c.insertDagScheduleQuery(), dagId, insertTs, event, schedule,
		nextSchedule,
	)
	if iErr != nil {
		c.logger.Error("Failed to insert new DAG schedule event", "dagId",
			dagId, "event", event, "scheduleTs", schedule, "nextScheduleTs",
			nextSchedule, "err", iErr.Error())
	}
	c.logger.Debug("Finished inserting new DAG schedule event", "dagId", dagId,
		"event", event, "scheduleTs", schedule, "nextScheduleTs", nextSchedule,
		"duration", time.Since(start))
	return nil
}

// Parses single Schedule row based on SQL query result.
func parseScheduleRow(rows *sql.Rows) (Schedule, error) {
	var dagId, insertTs, event, scheduleTs string
	var prevScheduleTs *string

	scanErr := rows.Scan(&dagId, &insertTs, &event, &prevScheduleTs, &scheduleTs)
	if scanErr != nil {
		return Schedule{}, scanErr
	}
	return Schedule{
		DagId:          dagId,
		InsertTs:       insertTs,
		Event:          event,
		ScheduleTs:     prevScheduleTs,
		NextScheduleTs: scheduleTs,
	}, nil
}

func (c *Client) insertDagScheduleQuery() string {
	return `
	INSERT INTO schedules(DagId, InsertTs, Event, ScheduleTs, NextScheduleTs)
	VALUES (?,?,?,?,?)
`
}

func (c *Client) readDagSchedulesQuery() string {
	return `
		SELECT
			DagId,
			InsertTs,
			Event,
			ScheduleTs,
			NextScheduleTs
		FROM
			schedules
		WHERE
			DagId = ?
		ORDER BY
			NextScheduleTs DESC
`
}
