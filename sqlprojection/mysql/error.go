package mysql

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

const (
	codeDupEntry = 1062 // https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_dup_entry
)

// isDuplicateEntry returns true if err represents a MySQL duplicate entry error.
func isDuplicateEntry(err error) bool {
	var e *mysql.MySQLError
	if errors.As(err, &e) {
		return e.Number == codeDupEntry
	}

	return false
}
