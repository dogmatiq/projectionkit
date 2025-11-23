package sqlprojection

import (
	"context"
	"database/sql"
)

// PostgresDriver is a Driver for PostgreSQL.
//
// This driver should work with any underlying Go SQL driver that supports
// PostgreSQL compatible databases and $1-style placeholders.
var PostgresDriver Driver = postgresDriver{}

type postgresDriver struct{}

func (postgresDriver) CreateSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint:errcheck

	if _, err := tx.ExecContext(
		ctx,
		`CREATE SCHEMA IF NOT EXISTS projection`,
	); err != nil {
		return err
	}

	// We define a function to convert from BYTEA to UUID on the server-side so
	// we are only sending 16 byte raw UUIDs over the write (instead of 36 byte
	// hex-encoded strings).
	if _, err = tx.ExecContext(
		ctx,
		`CREATE OR REPLACE FUNCTION projection.bytea_to_uuid (BYTEA) RETURNS UUID AS $$
			SELECT ENCODE($1, 'hex')::UUID;
		$$ LANGUAGE SQL IMMUTABLE;`); err != nil {
		return err
	}

	if _, err = tx.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS projection.checkpoint (
			handler           UUID NOT NULL,
			stream            UUID NOT NULL,
			checkpoint_offset BIGINT NOT NULL,

			PRIMARY KEY (handler, stream)
		)`,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (postgresDriver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS projection CASCADE`)
	return err
}

func (postgresDriver) QueryCheckpointOffset(
	ctx context.Context,
	db *sql.DB,
	h, s []byte,
) (uint64, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT checkpoint_offset
		FROM projection.checkpoint
		WHERE handler = projection.bytea_to_uuid($1)
		AND stream = projection.bytea_to_uuid($2)`,
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

func (postgresDriver) UpdateCheckpointOffset(
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
			`INSERT INTO projection.checkpoint (
				handler,
				stream,
				checkpoint_offset
			) VALUES (
				projection.bytea_to_uuid($1),
				projection.bytea_to_uuid($2),
				$3
			) ON CONFLICT DO NOTHING`,
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
		`UPDATE projection.checkpoint SET
			checkpoint_offset = $1
		WHERE handler = projection.bytea_to_uuid($2)
		AND stream = projection.bytea_to_uuid($3)
		AND checkpoint_offset = $4`,
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
func (postgresDriver) DeleteCheckpointOffsets(
	ctx context.Context,
	tx *sql.Tx,
	h []byte,
) error {
	_, err := tx.ExecContext(
		ctx,
		`DELETE FROM projection.checkpoint
		WHERE handler = $1`,
		h,
	)
	return err
}
