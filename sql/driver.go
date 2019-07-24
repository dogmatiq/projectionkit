package sql

import (
	"context"
	"database/sql"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
	Associate(
		ctx context.Context,
		tx *sql.Tx,
		h string,
		k, v []byte,
	) error

	Recover(
		ctx context.Context,
		db *sql.DB,
		h string,
		k []byte,
	) ([]byte, bool, error)

	Discard(
		ctx context.Context,
		db *sql.DB,
		h string,
		k []byte,
	) error
}
