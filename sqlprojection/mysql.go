package sqlprojection

import (
	"context"
	"database/sql"
)

// MySQLDriver is a Driver for MySQL.
//
// This driver should work with any underlying Go SQL driver that supports MySQL
// compatible databases and ?-style placeholders.
var MySQLDriver Driver = mysqlDriver{}

type mysqlDriver struct{}

func (mysqlDriver) CreateSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS projection_checkpoint (
			handler           BINARY(16) NOT NULL,
			stream            BINARY(16) NOT NULL,
			checkpoint_offset BIGINT UNSIGNED NOT NULL,

			PRIMARY KEY (handler, stream)
		) ENGINE=InnoDB;`,
	)
	return err
}

func (mysqlDriver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS projection_checkpoint`)
	return err
}

func (mysqlDriver) QueryCheckpointOffset(
	ctx context.Context,
	db *sql.DB,
	h, s []byte,
) (uint64, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT checkpoint_offset
		FROM projection_checkpoint
		WHERE handler = ?
		AND stream = ?`,
		h,
		s,
	)

	var cp uint64
	err := row.Scan(&cp)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	return cp, err
}

func (mysqlDriver) UpdateCheckpointOffset(
	ctx context.Context,
	tx *sql.Tx,
	h, s []byte,
	c, n uint64,
) (bool, error) {
	// If the "current" checkpoint offset is zero, we assumed it's correct and
	// that there is no existing row for this handler/stream.
	if c == 0 {
		res, err := tx.ExecContext(
			ctx,
			`INSERT INTO projection_checkpoint (
				handler,
				stream,
				checkpoint_offset
			) VALUES (
				?,
				?,
				?
			) ON DUPLICATE KEY UPDATE
				handler = handler`, // do nothing
			h,
			s,
			n,
		)
		if err != nil {
			return false, err
		}

		// The affected rows will be exactly 1 if the row was inserted.
		n, err := res.RowsAffected()
		return n == 1, err
	}

	var (
		res sql.Result
		err error
	)

	// Otherwise we simply update the existing row.
	res, err = tx.ExecContext(
		ctx,
		`UPDATE projection_checkpoint SET
			checkpoint_offset = ?
		WHERE handler = ?
		AND stream = ?
		AND checkpoint_offset = ?`,
		n,
		h,
		s,
		c,
	)

	if err != nil {
		return false, err
	}

	count, err := res.RowsAffected()
	return count != 0, err
}

// DeleteCheckpointOffsets deletes all checkpoint offsets for a specific
// handler.
func (mysqlDriver) DeleteCheckpointOffsets(
	ctx context.Context,
	tx *sql.Tx,
	h []byte,
) error {
	_, err := tx.ExecContext(
		ctx,
		`DELETE FROM projection_checkpoint
		WHERE handler = ?`,
		h,
	)
	return err
}
