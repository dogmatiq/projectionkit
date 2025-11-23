package sqlprojection_test

import (
	"os"
	"testing"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteDriver(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	os.Remove(file.Name())

	runTests(
		t,
		"sqlite3", "file:"+file.Name()+"?mode=rwc",
		SQLiteDriver,
	)
}
