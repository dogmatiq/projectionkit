// +build cgo

package sqlite

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

// isDuplicateEntry returns true if err represents a PostgreSQL unique
// constraint violation.
func isDuplicateEntry(err error) bool {
	var e sqlite3.Error
	if errors.As(err, &e) {
		return e.Code == sqlite3.ErrConstraint &&
			(e.ExtendedCode == sqlite3.ErrConstraintPrimaryKey ||
				e.ExtendedCode == sqlite3.ErrConstraintUnique)
	}

	return false
}
