package drivertest

import (
	"database/sql"
	"os"

	_ "github.com/go-sql-driver/mysql" // keep driver import near code that uses it
	_ "github.com/lib/pq"              // keep driver import near code that uses it
	_ "github.com/mattn/go-sqlite3"    // keep driver import near code that uses it

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DSN returns the DSN for the test database to use with the given SQL driver.
func DSN(driver string) string {
	var env, dsn string

	switch driver {
	case "mysql":
		env = "DOGMATIQ_TEST_MYSQL_DSN"
		dsn = "root:rootpass@tcp(127.0.0.1:3306)/dogmatiq"
	case "sqlite3":
		env = "DOGMATIQ_TEST_SQLITE_DSN"
		dsn = ":memory:"
	case "postgres":
		env = "DOGMATIQ_TEST_POSTGRES_DSN"
		dsn = "user=postgres password=rootpass sslmode=disable"
	default:
		ginkgo.Fail("unsupported driver: " + driver)
	}

	if v := os.Getenv(env); v != "" {
		return v
	}

	return dsn
}

// Open returns the test database to use with the given driver.
func Open(driver string) *sql.DB {
	dsn := DSN(driver)

	db, err := sql.Open(driver, dsn)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	if driver == "sqlite3" && dsn == ":memory:" {
		// Ensure that we only ever have one "connection" to the memory
		// database, otherwise each connection obtained from the pool works on
		// its own in-memory data store.
		db.SetMaxOpenConns(1)
	}

	return db
}
