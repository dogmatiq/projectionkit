package sqlite

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements required by the PostgreSQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`CREATE TABLE projection_occ (
			handler  BINARY NOT NULL,
			resource BINARY NOT NULL,
			version  BINARY NOT NULL,

			PRIMARY KEY (handler, resource)
		)`,
	)
	return err
}

// DropSchema drops the schema elements required by the PostgreSQL driver.
func DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS projection_occ`)
	return err
}
