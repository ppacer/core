package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ppacer/core/timeutils"
)

// DagRunRestart represents a row in dagrunrestarts table.
type DagRunRestart struct {
	DagId   string
	ExecTs  string
	Restart int
	InserTs string
}

// ReadDagRunRestartLatest reads latest restart event for given DAG run.
func (c *Client) ReadDagRunRestartLatest(
	ctx context.Context, dagId, execTs string,
) (DagRunRestart, error) {
	return readRow(
		ctx, c.dbConn, c.logger, parseDagRunRestartRow,
		c.readDagRunRestartLatestQuery(), dagId, execTs,
	)
}

// InsertDagRunNextRestart inserts new DAG run restart event. If given DAG run
// haven't been restarted yet, then record with Restart=1 will be inserted. In
// case when this is a second restart, then Restart=2 will be inserted and so
// on.
func (c *Client) InsertDagRunNextRestart(
	ctx context.Context, dagId, execTs string,
) error {
	start := time.Now()
	insertTs := timeutils.ToString(timeutils.Now())
	c.logger.Debug("Start inserting new DAG run restart event", "dagId", dagId,
		"execTs", execTs)
	res, iErr := c.dbConn.ExecContext(
		ctx, c.insertDagRunNextRestartQuery(), dagId, execTs, dagId, execTs,
		dagId, execTs, dagId, execTs, dagId, execTs, insertTs,
	)
	if iErr != nil {
		c.logger.Error("Cannot insert new DAG run restart event", "dagId",
			dagId, "execTs", execTs, "err", iErr)
		return iErr
	}
	rows, rowsErr := res.RowsAffected()
	if rowsErr != nil {
		return fmt.Errorf("cannot get number of affected rows: %w", rowsErr)
	}
	if rows > 1 {
		return fmt.Errorf("expected 1 row to be inserted for restarted DAG run (%s, %s), got %d",
			dagId, execTs, rows)
	}
	c.logger.Debug("Finished inserting new DAG run restart event", "dagId",
		dagId, "execTs", execTs, "duration", time.Since(start))
	return nil
}

func parseDagRunRestartRow(row Scannable) (DagRunRestart, error) {
	var dr DagRunRestart
	err := row.Scan(&dr.DagId, &dr.ExecTs, &dr.Restart, &dr.InserTs)
	return dr, err
}

func (c *Client) readDagRunRestartLatestQuery() string {
	return `
	SELECT
		DagId, ExecTs, Restart, InsertTs
	FROM
		dagrunrestarts
	WHERE
		DagId = ? AND ExecTs = ?
	ORDER BY
		Restart DESC
	LIMIT 1
`
}

func (c *Client) insertDagRunNextRestartQuery() string {
	return `
	WITH latestRestart AS (
		SELECT
			DagId, ExecTs, MAX(Restart) AS LatestRestart
		FROM
			dagrunrestarts
		WHERE
			DagId = ? AND ExecTs = ?
		GROUP BY
			DagId, ExecTs

		UNION ALL
		
		SELECT
			? AS DagId,
			? AS ExecTs,
			0 AS LatestRestart
		WHERE
			NOT EXISTS (
				SELECT 1
				FROM dagrunrestarts
				WHERE DagId = ? AND ExecTs = ?
			)
	)
	INSERT INTO dagrunrestarts(DagId, ExecTs, Restart, InsertTs)
	SELECT
		? AS DagId,
		? AS ExecTs,
		COALESCE(LatestRestart, 0) + 1 AS Restart,
		? AS InsertTs
	FROM
		latestRestart
`
}
