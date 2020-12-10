package sqlprojection

import (
	"context"
	"database/sql"
	"fmt"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
	// IsCompatibleWith returns true if this driver can be used to store
	// projections on db.
	IsCompatibleWith(db *sql.DB) bool

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

// drivers is a list of the built-in drivers.
var drivers = []Driver{
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
func NewDriver(db *sql.DB) (Driver, error) {
	for _, d := range drivers {
		if d.IsCompatibleWith(db) {
			return d, nil
		}
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL projection driver for %T",
		db.Driver(),
	)
}
