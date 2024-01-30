// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ppacer/core/dag"
	"github.com/ppacer/core/timeutils"
	"github.com/ppacer/core/version"
)

type Dag struct {
	DagId               string
	StartTs             *string
	Schedule            *string
	CreateTs            string
	LatestUpdateTs      *string
	CreateVersion       string
	LatestUpdateVersion *string
	HashDagMeta         string
	HashTasks           string
	Attributes          string // serialized dag.Dag.Attr
}

// ReadDag reads metadata about DAG from dags table for given dagId.
func (c *Client) ReadDag(ctx context.Context, dagId string) (Dag, error) {
	tx, _ := c.dbConn.Begin()
	d, err := c.readDagTx(ctx, tx, dagId)
	cErr := tx.Commit()
	if cErr != nil {
		slog.Error("Could not commit SQL transaction", "dagId", dagId, "err",
			cErr)
		return Dag{}, cErr
	}
	return d, err
}

// Upsert inserts or updates DAG details in dags table.
// TODO(dskrzypiec): Perhaps we should always insert new DAG into dags and keep
// IsCurrent flag? Similarly like we do in dagtasks. Not really needed for now,
// but something to consider in the future.
func (c *Client) UpsertDag(ctx context.Context, d dag.Dag) error {
	start := time.Now()
	insertTs := timeutils.ToString(time.Now())
	dagId := string(d.Id)
	slog.Debug("Start upserting dag in dags table...", "dagId", dagId,
		"insertTs", insertTs)
	tx, _ := c.dbConn.Begin()

	// Check if there is already a record for given DAG
	currDagRow, currErr := c.readDagTx(ctx, tx, dagId)
	if currErr == sql.ErrNoRows {
		// If no, then simply insert
		dag := fromDagToDag(d, insertTs)
		iErr := c.insertDag(ctx, tx, dag, insertTs)
		cErr := tx.Commit()
		if cErr != nil {
			slog.Error("Could not commit SQL transaction", "dagId", dagId,
				"err", cErr)
			tx.Rollback()
			return cErr
		}
		slog.Debug("Inserted new DAG into dags table", "dagId", dagId,
			"duration", time.Since(start))
		return iErr
	}
	// Otherwise we need to update existing entry in dags table
	updatedDag := dagUpdate(d, currDagRow, insertTs)
	uErr := c.updateDag(ctx, tx, updatedDag)

	cErr := tx.Commit()
	if cErr != nil {
		slog.Error("Could not commit SQL transaction", "dagId", dagId, "err",
			cErr)
		tx.Rollback()
		return cErr
	}
	slog.Debug("Updating DAG row in dags table", "dagId", dagId, "duration",
		time.Since(start))
	return uErr
}

// readDag reads a row from dags table within SQL transaction.
func (c *Client) readDagTx(ctx context.Context, tx *sql.Tx, dagId string) (Dag, error) {
	start := time.Now()
	slog.Debug("Start reading dag from dags table", "dagId", dagId)

	row := tx.QueryRowContext(ctx, c.readDagQuery(), dagId)
	var dId, createTs, createVersion, hashMeta, hashTasks, attr string
	var startTs, schedule, latestUpdateTs, latestUpdateVersion *string

	scanErr := row.Scan(&dId, &startTs, &schedule, &createTs, &latestUpdateTs,
		&createVersion, &latestUpdateVersion, &hashMeta, &hashTasks, &attr)
	if scanErr == sql.ErrNoRows {
		return Dag{}, scanErr
	}
	if scanErr != nil {
		slog.Error("Failed scanning dag record", "dagId", dagId, "err", scanErr)
		return Dag{}, scanErr
	}
	dag := Dag{
		DagId:               dId,
		StartTs:             startTs,
		Schedule:            schedule,
		CreateTs:            createTs,
		LatestUpdateTs:      latestUpdateTs,
		CreateVersion:       createVersion,
		LatestUpdateVersion: latestUpdateVersion,
		HashDagMeta:         hashMeta,
		HashTasks:           hashTasks,
		Attributes:          attr,
	}
	slog.Debug("Finished reading dag from dags table", "dagId", dagId, "duration",
		time.Since(start))
	return dag, nil
}

// Insert new row in dags table.
func (c *Client) insertDag(ctx context.Context, tx *sql.Tx, d Dag, insertTs string) error {
	_, err := tx.ExecContext(
		ctx,
		c.dagInsertQuery(),
		d.DagId, d.StartTs, d.Schedule, d.CreateTs, d.LatestUpdateTs, d.CreateVersion, d.LatestUpdateVersion, d.HashDagMeta,
		d.HashTasks, d.Attributes,
	)
	if err != nil {
		return err
	}
	return nil
}

// Updates existing row in dags table.
func (c *Client) updateDag(ctx context.Context, tx *sql.Tx, d Dag) error {
	_, err := tx.ExecContext(
		ctx,
		c.dagUpdateQuery(),
		d.StartTs, d.Schedule, d.LatestUpdateTs, d.LatestUpdateVersion, d.HashDagMeta, d.HashTasks, d.Attributes, d.DagId,
	)
	if err != nil {
		return err
	}
	return nil
}

func fromDagToDag(d dag.Dag, createTs string) Dag {
	attrJson, jErr := json.Marshal(d.Attr)
	if jErr != nil {
		attrJson = []byte("FAILED DAG ATTR SERIALIZATION")
	}
	dagStart, sched := dagStartAndScheduleStrings(d)
	return Dag{
		DagId:               string(d.Id),
		StartTs:             dagStart,
		Schedule:            sched,
		CreateTs:            createTs,
		LatestUpdateTs:      nil,
		CreateVersion:       version.Version,
		LatestUpdateVersion: nil,
		HashDagMeta:         d.HashDagMeta(),
		HashTasks:           d.HashTasks(),
		Attributes:          string(attrJson),
	}
}

func dagUpdate(d dag.Dag, currDagRow Dag, insertTs string) Dag {
	attrJson, jErr := json.Marshal(d.Attr)
	if jErr != nil {
		attrJson = []byte("FAILED DAG ATTR SERIALIZATION")
	}
	dagStart, sched := dagStartAndScheduleStrings(d)
	return Dag{
		DagId:               string(d.Id),
		StartTs:             dagStart,
		Schedule:            sched,
		CreateTs:            currDagRow.CreateTs,
		LatestUpdateTs:      &insertTs,
		CreateVersion:       currDagRow.CreateVersion,
		LatestUpdateVersion: &version.Version,
		HashDagMeta:         d.HashDagMeta(),
		HashTasks:           d.HashTasks(),
		Attributes:          string(attrJson),
	}
}

func dagStartAndScheduleStrings(d dag.Dag) (*string, *string) {
	var dagStart, sched *string
	if d.Schedule != nil {
		schedStr := (*d.Schedule).String()
		sched = &schedStr
		startStr := timeutils.ToString((*d.Schedule).StartTime())
		dagStart = &startStr
	}
	return dagStart, sched
}

func (c *Client) readDagQuery() string {
	return `
		SELECT
			DagId,
			StartTs,
			Schedule,
			CreateTs,
			LatestUpdateTs,
			CreateVersion,
			LatestUpdateVersion,
			HashDagMeta,
			HashTasks,
			Attributes
		FROM
			dags
		WHERE
			DagId = ?
	`
}

func (c *Client) dagInsertQuery() string {
	return `
		INSERT INTO dags (
			DagId, StartTs, Schedule, CreateTs, LatestUpdateTs, CreateVersion,
			LatestUpdateVersion, HashDagMeta, HashTasks, Attributes
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
}

func (c *Client) dagUpdateQuery() string {
	return `
		UPDATE
			dags
		SET
			StartTs = ?,
			Schedule = ?,
			LatestUpdateTs = ?,
			LatestUpdateVersion = ?,
			HashDagMeta = ?,
			HashTasks = ?,
			Attributes = ?
		WHERE
			DagId = ?
	`
}

// TODO: Move somewhere?
func pointerEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func (d Dag) Equals(e Dag) bool {
	if d.DagId != e.DagId {
		return false
	}
	if !pointerEqual(d.StartTs, e.StartTs) {
		return false
	}
	if !pointerEqual(d.Schedule, e.Schedule) {
		return false
	}
	if d.CreateTs != e.CreateTs {
		return false
	}
	if !pointerEqual(d.LatestUpdateTs, e.LatestUpdateTs) {
		return false
	}
	if d.CreateVersion != e.CreateVersion {
		return false
	}
	if !pointerEqual(d.LatestUpdateVersion, e.LatestUpdateVersion) {
		return false
	}
	if d.HashDagMeta != e.HashDagMeta {
		return false
	}
	if d.HashTasks != e.HashTasks {
		return false
	}
	if d.Attributes != e.Attributes {
		return false
	}
	return true
}
