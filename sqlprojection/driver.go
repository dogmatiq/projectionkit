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

// builtInDrivers is a list of the built-in drivers.
var builtInDrivers = []Driver{
	MySQLDriver,
	PostgresDriver,
	SQLiteDriver,
}

// NewDriver returns the appropriate driver implementation to use with the given
// database.
//
// The following database products and SQL drivers are officially supported:
//
// MySQL and MariaDB via the "mysql" ("github.com/go-sql-driver/mysql") driver.
//
// PostgreSQL via the "postgres" (github.com/lib/pq) and "pgx"
// (github.com/jackc/pgx) drivers.
//
// SQLite via the "sqlite3" (github.com/mattn/go-sqlite3) driver (requires CGO).
func NewDriver(ctx context.Context, db *sql.DB) (Driver, error) {
	return selectDriver(ctx, db, builtInDrivers)
}

func selectDriver(ctx context.Context, db *sql.DB, candidates []Driver) (Driver, error) {
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
