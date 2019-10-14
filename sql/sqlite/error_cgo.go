// +build cgo

package sqlite

import "github.com/mattn/go-sqlite3"

// isDuplicateEntry returns true if err represents a PostgreSQL unique
// constraint violation.
func isDuplicateEntry(err error) bool {
	e, ok := err.(sqlite3.Error)
	return ok &&
		e.Code == sqlite3.ErrConstraint &&
		(e.ExtendedCode == sqlite3.ErrConstraintPrimaryKey ||
			e.ExtendedCode == sqlite3.ErrConstraintUnique)
}
