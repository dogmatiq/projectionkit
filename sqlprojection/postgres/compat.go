package postgres

import (
	"database/sql"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/lib/pq"
)

// IsCompatibleWith returns true if the driver implemented in this package is
// compatible with the given database pool.
func IsCompatibleWith(db *sql.DB) bool {
	switch db.Driver().(type) {
	case *pq.Driver:
		return true
	case *stdlib.Driver:
		return true
	default:
		return false
	}
}
