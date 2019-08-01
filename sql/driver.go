package sql

import (
	"context"
	"database/sql"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
	UpdateVersion(
		ctx context.Context,
		tx *sql.Tx,
		h string,
		r, c, n []byte,
	) (bool, error)

	ResourceVersion(
		ctx context.Context,
		db *sql.DB,
		h string,
		r []byte,
	) ([]byte, error)

	CloseResource(
		ctx context.Context,
		db *sql.DB,
		h string,
		r []byte,
	) error
}
