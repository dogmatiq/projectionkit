package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"strings"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/sqlite"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var db *sql.DB

	BeforeSuite(func() {
		dsn := os.Getenv("DOGMATIQ_TEST_SQLITE_DSN")
		if dsn == "" {
			dsn = ":memory:"
		}

		var err error
		db, err = sql.Open("sqlite3", dsn)
		Expect(err).ShouldNot(HaveOccurred())

		// Ensure that we only ever have one "connection" to the memory
		// database, otherwise the only be created in one of them.
		db.SetMaxOpenConns(1)
	})

	AfterSuite(func() {
		if db != nil {
			db.Close()
		}
	})

	drivertest.Declare(
		&Driver{},
		func(ctx context.Context) *sql.DB {
			err := DropSchema(ctx, db)
			if err != nil && strings.Contains(err.Error(), "CGO_ENABLED") {
				Skip(err.Error())
			}
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return db
		},
	)
})
