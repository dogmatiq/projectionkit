package postgres

import (
	"context"
	"database/sql"
)

// Driver is an implementation of sql.Driver for PostgreSQL.
type Driver struct{}

// UpdateVersion updates the version for a specific handler and resource.
func (*Driver) UpdateVersion(
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
			`INSERT INTO projection.occ (
				handler,
				resource,
				version
			) VALUES (
				$1,
				$2,
				$3
			)`,
			h,
			r,
			n,
		)

		// If this results in a duplicate key error, that means the current
		// version was not correct.
		if isDuplicateEntry(err) {
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

// QueryVersion returns the version for a specific handler and resource.
func (*Driver) QueryVersion(
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

// DeleteResource removes the version for a specific handler and resource.
func (*Driver) DeleteResource(
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
