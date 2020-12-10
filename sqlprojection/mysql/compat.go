package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

// IsCompatibleWith returns true if the driver implemented in this package is
// compatible with the given database pool.
func IsCompatibleWith(db *sql.DB) bool {
	_, ok := db.Driver().(*mysql.MySQLDriver)
	return ok
}
