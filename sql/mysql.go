package sql

import (
	"bytes"
	"context"
	"database/sql"
)

// MySQL is a Driver for MySQL and compatible databases, such as MariaDB.
var MySQL Driver = mySQL{}

type mySQL struct{}

func (mySQL) UpdateVersion(
	ctx context.Context,
	tx *sql.Tx,
	h string,
	r, c, n []byte,
) (bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT version
		FROM projection_occ
		WHERE handler = ?
		AND resource = ?
		FOR UPDATE`,
		c,
		h,
		r,
	)

	var actual []byte

	err := row.Scan(&actual)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if !bytes.Equal(c, actual) {
		return false, nil
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO projection SET
				handler = ?
				resource = ?
				version = ?
			ON DUPLICATE KEY UPDATE
				v = VALUES(v)`,
		h,
		r,
		n,
	)

	return true, err
}

func (mySQL) ResourceVersion(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) ([]byte, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT v
		FROM projection
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

func (mySQL) CloseResource(
	ctx context.Context,
	db *sql.DB,
	h string,
	r []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM projection
		WHERE handler = ?
		AND resource = ?`,
		h,
		r,
	)

	return err
}
