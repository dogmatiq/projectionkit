package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/mattn/go-sqlite3"
)

// SQLiteDriver is Driver for SQLite.
var SQLiteDriver Driver = sqliteDriver{}

type sqliteDriver struct{}

func (sqliteDriver) IsCompatibleWith(db *sql.DB) bool {
	_, ok := db.Driver().(*sqlite3.SQLiteDriver)
	return ok
}

func (sqliteDriver) CreateSchema(ctx context.Context, db *sql.DB) error {
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

func (sqliteDriver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS projection_occ`)
	return err
}

func (sqliteDriver) StoreVersion(
	ctx context.Context,
	db *sql.DB,
	h string,
	r, v []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`INSERT INTO projection_occ (
			handler,
			resource,
			version
		) VALUES (
			?,
			?,
			?
		) ON CONFLICT (handler, resource) DO UPDATE SET
			version = excluded.version`,
		h,
		r,
		v,
	)

	return err
}

func (d sqliteDriver) UpdateVersion(
	ctx context.Context,
	tx *sql.Tx,
	h string,
	r, c, n []byte,
) (bool, error) {
	// If the "current" version is empty, we assumed it's correct and that there
	// is no existing entry for this resource.
	if len(c) == 0 {
		_, err := tx.ExecContext(
			ctx,
			`INSERT INTO projection_occ (
				handler,
				resource,
				version
			) VALUES (
				?,
				?,
				?
			)`,
			h,
			r,
			n,
		)

		// If this results in a duplicate key error, that means the current
		// version was not correct.
		if d.isDup(err) {
			return false, nil
		}

		return true, err
	}

	var (
		res sql.Result
		err error
	)

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the row entirely.
		res, err = tx.ExecContext(
			ctx,
			`DELETE FROM projection_occ
			WHERE handler = ?
			AND resource = ?
			AND version = ?`,
			h,
			r,
			c,
		)
	} else {
		// Otherwise we simply update the existing row.
		res, err = tx.ExecContext(
			ctx,
			`UPDATE projection_occ SET
				version = ?
			WHERE handler = ?
			AND resource = ?
			AND version = ?`,
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

func (sqliteDriver) QueryVersion(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) ([]byte, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			version
		FROM projection_occ
		WHERE handler = ?
		AND resource = ?`,
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

func (sqliteDriver) DeleteResource(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM projection_occ
		WHERE handler = ?
		AND resource = ?`,
		h,
		r,
	)

	return err
}
