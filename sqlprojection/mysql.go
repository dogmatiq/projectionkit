package sqlprojection

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQLDriver is a Driver for MySQL and compatible databases such as MariaDB.
var MySQLDriver Driver = mysqlDriver{}

type mysqlDriver struct{}

func (mysqlDriver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that ?-style placeholders are supported.
	err := db.QueryRowContext(
		ctx,
		`SELECT 1 WHERE 1 = ?`,
		1,
	).Err()

	if err != nil {
		return err
	}

	// Verify that we're using something compatible with MySQL (because the SHOW
	// VARIABLES syntax is supported) and that InnoDB is available.
	return db.QueryRowContext(
		ctx,
		`SHOW VARIABLES LIKE "innodb_page_size"`,
	).Err()
}

func (mysqlDriver) CreateSchema(ctx context.Context, db *sql.DB) error {
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

func (mysqlDriver) DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS projection_occ`)
	return err
}

func (mysqlDriver) StoreVersion(
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
		) ON DUPLICATE KEY UPDATE
			version = VALUES(version)`,
		h,
		r,
		v,
	)

	return err
}

func (d mysqlDriver) UpdateVersion(
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

func (mysqlDriver) QueryVersion(
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

func (mysqlDriver) DeleteResource(
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

func (mysqlDriver) isDup(err error) bool {
	var e *mysql.MySQLError
	if errors.As(err, &e) {
		// https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_dup_entry
		return e.Number == 1062
	}

	return false
}
