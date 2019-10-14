package postgres

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements required by the PostgreSQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`CREATE SCHEMA projection;
		CREATE TABLE projection.occ (
			handler  BYTEA NOT NULL,
			resource BYTEA NOT NULL,
			version  BYTEA NOT NULL,

			PRIMARY KEY (handler, resource)
		);`,
	)
	return err
}

// DropSchema drops the schema elements required by the PostgreSQL driver.
func DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS projection CASCADE`)
	return err
}
