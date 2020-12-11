package sqlprojection

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/multierr"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
	// IsCompatibleWith returns nil if this driver can be used to store
	// projections on db.
	IsCompatibleWith(ctx context.Context, db *sql.DB) error

	// CreateSchema creates the schema elements required by the driver.
	CreateSchema(ctx context.Context, db *sql.DB) error

	// DropSchema drops the schema elements required by the driver.
	DropSchema(ctx context.Context, db *sql.DB) error

	// StoreVersion unconditionally updates the version for a specific handler
	// and resource.
	//
	// v must be non-empty, to set an empty version, use DeleteResource().
	StoreVersion(
		ctx context.Context,
		db *sql.DB,
		h string,
		r, v []byte,
	) error

	// UpdateVersion updates the version for a specific handler and resource.
	UpdateVersion(
		ctx context.Context,
		tx *sql.Tx,
		h string,
		r, c, n []byte,
	) (bool, error)

	// QueryVersion returns the version for a specific handler and resource.
	QueryVersion(
		ctx context.Context,
		db *sql.DB,
		h string,
		r []byte,
	) ([]byte, error)

	// DeleteResource removes the version for a specific handler and resource.
	DeleteResource(
		ctx context.Context,
		db *sql.DB,
		h string,
		r []byte,
	) error
}

// BuiltInDrivers returns a list of the built-in drivers.
func BuiltInDrivers() []Driver {
	return []Driver{
		MySQLDriver,
		PostgresDriver,
		SQLiteDriver,
	}
}

// SelectDriver returns the appropriate driver implementation to use with the
// given database from a list of candidate drivers.
func SelectDriver(ctx context.Context, db *sql.DB, candidates []Driver) (Driver, error) {
	var err error

	for _, d := range candidates {
		e := d.IsCompatibleWith(ctx, db)
		if e == nil {
			return d, nil
		}

		err = multierr.Append(err, fmt.Errorf(
			"%T is not compatible with %T: %w",
			d,
			db.Driver(),
			e,
		))
	}

	return nil, multierr.Append(err, fmt.Errorf(
		"none of the candidate drivers are compatible with %T",
		db.Driver(),
	))
}
