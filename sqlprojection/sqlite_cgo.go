// +build cgo

package sqlprojection

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

func (sqliteDriver) isDup(err error) bool {
	var e sqlite3.Error
	if errors.As(err, &e) {
		return e.Code == sqlite3.ErrConstraint &&
			(e.ExtendedCode == sqlite3.ErrConstraintPrimaryKey ||
				e.ExtendedCode == sqlite3.ErrConstraintUnique)
	}

	return false
}
