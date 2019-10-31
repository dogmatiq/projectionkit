package postgres

import (
	"database/sql"

	"github.com/lib/pq"
)

// IsCompatibleWith returns true if the driver implemented in this package is
// compatible with the given database pool.
func IsCompatibleWith(db *sql.DB) bool {
	_, ok := db.Driver().(*pq.Driver)
	return ok
}
