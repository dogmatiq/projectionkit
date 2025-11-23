package sqlprojection

import (
	"context"
	"database/sql"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
	// CreateSchema creates the schema elements required by the driver.
	CreateSchema(ctx context.Context, db *sql.DB) error

	// DropSchema drops the schema elements required by the driver.
	DropSchema(ctx context.Context, db *sql.DB) error

	// QueryCheckpointOffset returns the stored checkpoint offset for a specific
	// handler and event stream.
	QueryCheckpointOffset(
		ctx context.Context,
		db *sql.DB,
		h, s []byte,
	) (uint64, error)

	// UpdateCheckpointOffset updates the checkpoint offset for a specific
	// handler and event stream from c to n.
	//
	// It returns false if c is not the current checkpoint offset.
	UpdateCheckpointOffset(
		ctx context.Context,
		tx *sql.Tx,
		h, s []byte,
		c, n uint64,
	) (bool, error)

	// DeleteCheckpointOffsets deletes all checkpoint offsets for a specific
	// handler.
	DeleteCheckpointOffsets(
		ctx context.Context,
		tx *sql.Tx,
		h []byte,
	) error
}
