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

func (postgresDriver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that we're using PostgreSQL and that $1-style placeholders are
	// supported.
	return db.QueryRowContext(
		ctx,
		`SELECT pg_backend_pid() WHERE 1 = $1`,
		1,
	).Err()
}

func (postgresDriver) CreateSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `CREATE SCHEMA projection`)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `CREATE TABLE projection.occ (
			handler  BYTEA NOT NULL,
			resource BYTEA NOT NULL,
			version  BYTEA NOT NULL,

			PRIMARY KEY (handler, resource)
		)`,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (postgresDriver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS projection CASCADE`)
	return err
}

func (postgresDriver) StoreVersion(
	ctx context.Context,
	db *sql.DB,
	h string,
	r, v []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`INSERT INTO projection.occ (
			handler,
			resource,
			version
		) VALUES (
			$1,
			$2,
			$3
		) ON CONFLICT (handler, resource) DO UPDATE SET
			version = excluded.version`,
		h,
		r,
		v,
	)

	return err
}

func (d postgresDriver) UpdateVersion(
	ctx context.Context,
	tx *sql.Tx,
	h string,
	r, c, n []byte,
) (bool, error) {
	// If the "current" version is empty, we assumed it's correct and that there
	// is no existing entry for this resource.
	if len(c) == 0 {
		res, err := tx.ExecContext(
			ctx,
			`INSERT INTO projection.occ (
				handler,
				resource,
				version
			) VALUES (
				$1,
				$2,
				$3
			) ON CONFLICT DO NOTHING`,
			h,
			r,
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

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the row entirely.
		res, err = tx.ExecContext(
			ctx,
			`DELETE FROM projection.occ
			WHERE handler = $1
			AND resource = $2
			AND version = $3`,
			h,
			r,
			c,
		)
	} else {
		// Otherwise we simply update the existing row.
		res, err = tx.ExecContext(
			ctx,
			`UPDATE projection.occ SET
				version = $1
			WHERE handler = $2
			AND resource = $3
			AND version = $4`,
			n,
			h,
			r,
			c,
		)
	}

	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the SQL connection or the schema in some way.
		return false, err
	}

	count, err := res.RowsAffected()
	return count != 0, err
}

func (postgresDriver) QueryVersion(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) ([]byte, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			version
		FROM projection.occ
		WHERE handler = $1
		AND resource = $2`,
		h,
		r,
	)

	var v []byte
	err := row.Scan(&v)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return v, err
}

func (postgresDriver) DeleteResource(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM projection.occ
		WHERE handler = $1
		AND resource = $2`,
		h,
		r,
	)

	return err
}
