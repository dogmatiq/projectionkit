package sql

import (
	"context"
	"database/sql"
)

// MySQL is a Driver for MySQL and compatible databases, such as MariaDB.
var MySQL Driver = mySQL{}

type mySQL struct{}

func (mySQL) Associate(
	ctx context.Context,
	tx *sql.Tx,
	h string,
	k, v []byte,
) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO projection SET
			handler = ?
			k = ?
			v = ?
		ON DUPLICATE KEY UPDATE
			v = VALUES(v)`,
		h,
		k,
		v,
	)

	return err
}

func (mySQL) Recover(
	ctx context.Context,
	db *sql.DB,
	h string,
	k []byte,
) (v []byte, ok bool, err error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT v
		FROM projection
		WHERE handler = ?
		AND k = ?`,
		h,
		k,
	)

	err = row.Scan(&v)

	if err == sql.ErrNoRows {
		err = nil
	} else {
		ok = true
	}

	return
}

func (mySQL) Discard(
	ctx context.Context,
	db *sql.DB,
	h string,
	k []byte,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM projection
		WHERE handler = ?
		AND k = ?`,
		h,
		k,
	)

	return err
}
