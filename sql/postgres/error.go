package postgres

import "github.com/lib/pq"

const (
	codeUniqueViolation = "23505" // https://www.postgresql.org/docs/10/errcodes-appendix.html
)

// isDuplicateEntry returns true if err represents a PostgreSQL unique
// constraint violation.
func isDuplicateEntry(err error) bool {
	e, ok := err.(*pq.Error)
	return ok && e.Code == codeUniqueViolation
}
