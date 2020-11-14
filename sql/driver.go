package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/projectionkit/sql/mysql"
	"github.com/dogmatiq/projectionkit/sql/postgres"
	"github.com/dogmatiq/projectionkit/sql/sqlite"
)

// Driver is an interface for database-specific projection drivers.
type Driver interface {
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

// NewDriver returns the appropriate driver implementation to use with the given
// database.
//
// The following database products and SQL drivers are officially supported:
//
// MySQL and MariaDB via the "mysql" ("github.com/go-sql-driver/mysql") driver.
//
// PostgreSQL and CockroachDB via the "postgres" (github.com/lib/pq) and "pgx"
// (github.com/jackc/pgx) drivers.
//
// SQLite via the "sqlite3" (github.com/mattn/go-sqlite3) driver (requires CGO).
func NewDriver(db *sql.DB) (Driver, error) {
	if mysql.IsCompatibleWith(db) {
		return &mysql.Driver{}, nil
	}

	if postgres.IsCompatibleWith(db) {
		return &postgres.Driver{}, nil
	}

	if sqlite.IsCompatibleWith(db) {
		return &sqlite.Driver{}, nil
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL projection driver for %T",
		db.Driver(),
	)
}
