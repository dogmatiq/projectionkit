package drivertest

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql" // keep driver import near code that uses it
	_ "github.com/jackc/pgx/v4/stdlib" // keep driver import near code that uses it
	_ "github.com/lib/pq"              // keep driver import near code that uses it
	_ "github.com/mattn/go-sqlite3"    // keep driver import near code that uses it
)

// DSN returns the DSN for the test database to use with the given SQL driver.
//
// The returned function must be used to cleanup any data created for the DSN,
// such as temporary on-disk databases.
func DSN(driver string) (string, func()) {
	dsn := dsnFromEnv(driver)
	if dsn != "" {
		return dsn, func() {}
	}

	switch driver {
	case "mysql":
		return "root:rootpass@tcp(127.0.0.1:3306)/dogmatiq", func() {}
	case "postgres":
		return "user=postgres password=rootpass sslmode=disable", func() {}
	case "pgx":
		return "postgres://postgres:rootpass@127.0.0.1:5432/?sslmode=disable", func() {}
	default:
		file, close := tempFile()
		return fmt.Sprintf("file:%s?mode=rwc", file), close
	}
}

// Open returns the test database to use with the given driver.
//
// The returned function must be used to close the database, instead of
// DB.Close().
func Open(
	driver string,
) (
	db *sql.DB,
	dsn string,
	close func(),
) {
	dsn, closeDSN := DSN(driver)

	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}

	return db, dsn, func() {
		db.Close()
		closeDSN()
	}
}

// dsnFromEnv returns a DSN for the given driver from an environment variable.
func dsnFromEnv(driver string) string {
	switch driver {
	case "mysql":
		return os.Getenv("DOGMATIQ_TEST_MYSQL_DSN")
	case "postgres":
		return os.Getenv("DOGMATIQ_TEST_POSTGRES_PQ_DSN")
	case "pgx":
		return os.Getenv("DOGMATIQ_TEST_POSTGRES_PGX_DSN")
	case "sqlite3":
		return os.Getenv("DOGMATIQ_TEST_SQLITE_DSN")
	default:
		panic("unsupported driver: " + driver)
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
