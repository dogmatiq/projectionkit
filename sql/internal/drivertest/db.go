package drivertest

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql" // keep driver import near code that uses it
	_ "github.com/jackc/pgx/v4/stdlib" // keep driver import near code that uses it
	_ "github.com/lib/pq"              // keep driver import near code that uses it
	_ "github.com/mattn/go-sqlite3"    // keep driver import near code that uses it
)

// Product is an enumeration of supported database products.
type Product string

const (
	// CockroachDB is the product enumeration value for CockroachDB.
	CockroachDB Product = "cockroachdb"

	// MySQL is the product enumeration value for MariaDB.
	MySQL Product = "mysql"

	// MariaDB is the product enumeration value for MariaDB.
	MariaDB Product = "mariadb"

	// PostgreSQL is the product enumeration value for PostgreSQL.
	PostgreSQL Product = "postgresql"

	// SQLite is the product enumeration value for SQLite.
	SQLite Product = "sqlite"
)

// DSN returns the DSN for the test database to use with the given database
// product and SQL driver.
//
// The returned function must be used to cleanup any data created for the DSN,
// such as temporary on-disk databases.
func DSN(prod Product, driver string) (string, func()) {
	key := strings.ToUpper(
		fmt.Sprintf(
			"DOGMATIQ_TEST_DSN_%s_%s",
			prod,
			driver,
		),
	)

	if dsn := os.Getenv(key); dsn != "" {
		return dsn, func() {}
	}

	switch key {
	case "DOGMATIQ_TEST_DSN_MYSQL_MYSQL":
		return "root:rootpass@tcp(127.0.0.1:3306)/dogmatiq", func() {}
	case "DOGMATIQ_TEST_DSN_MARIADB_MYSQL":
		return "root:rootpass@tcp(127.0.0.1:3307)/dogmatiq", func() {}
	case "DOGMATIQ_TEST_DSN_POSTGRESQL_POSTGRES":
		return "user=postgres password=rootpass sslmode=disable", func() {}
	case "DOGMATIQ_TEST_DSN_POSTGRESQL_PGX":
		return "postgres://postgres:rootpass@127.0.0.1:5432/?sslmode=disable", func() {}
	case "DOGMATIQ_TEST_DSN_COCKROACHDB_POSTGRES":
		return "user=root sslmode=disable port=26257", func() {}
	case "DOGMATIQ_TEST_DSN_COCKROACHDB_PGX":
		return "postgres://root@127.0.0.1:26257/?sslmode=disable", func() {}
	case "DOGMATIQ_TEST_DSN_SQLITE_SQLITE3":
		file, close := tempFile()
		return fmt.Sprintf("file:%s?mode=rwc", file), close
	default:
		panic(fmt.Sprintf("unsupported product (%s) or driver (%s)", prod, driver))
	}
}

// Open returns the test database to use with the given driver.
//
// The returned function must be used to close the database, instead of
// DB.Close().
func Open(
	prod Product,
	driver string,
) (
	db *sql.DB,
	dsn string,
	close func(),
) {
	dsn, closeDSN := DSN(prod, driver)

	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}

	return db, dsn, func() {
		db.Close()
		closeDSN()
	}
}

// tempFile returns the name of a temporary file to be used for an SQLite
// database.
//
// It returns a function that deletes the temporary file.
func tempFile() (string, func()) {
	f, err := ioutil.TempFile("", "*.sqlite3")
	if err != nil {
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}

	file := f.Name()

	if err := os.Remove(file); err != nil {
		panic(err)
	}

	var once sync.Once
	return file, func() {
		once.Do(func() {
			os.Remove(file)
		})
	}
}
