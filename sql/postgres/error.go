package postgres

import (
	"errors"

	"github.com/lib/pq"
)

const (
	codeUniqueViolation = "23505" // https://www.postgresql.org/docs/10/errcodes-appendix.html
)

// isDuplicateEntry returns true if err represents a PostgreSQL unique
// constraint violation.
func isDuplicateEntry(err error) bool {
	var e *pq.Error
	if errors.As(err, &e) {
		return e.Code == codeUniqueViolation
	}

	return false
}
