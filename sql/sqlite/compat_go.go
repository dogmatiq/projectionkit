// +build !cgo

package sqlite

import (
	"database/sql"

	"github.com/mattn/go-sqlite3"
)

// IsCompatibleWith returns true if the driver implemented in this package is
// compatible with the given database pool.
func IsCompatibleWith(db *sql.DB) bool {
	_, ok := db.Driver().(*sqlite3.SQLiteDriverMock)
	return ok
}
