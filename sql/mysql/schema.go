package mysql

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements required by the MySQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`CREATE TABLE projection_occ (
			handler  VARBINARY(255) NOT NULL,
			resource VARBINARY(255) NOT NULL,
			version  VARBINARY(255) NOT NULL,

			PRIMARY KEY (handler, resource)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4;`,
	)
	return err
}

// DropSchema drops the schema elements required by the MySQL driver.
func DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS projection_occ`)
	return err
}
