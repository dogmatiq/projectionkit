package postgres_test

import (
	"context"
	"database/sql"
	"os"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/postgres"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var db *sql.DB

	BeforeSuite(func() {
		dsn := os.Getenv("DOGMATIQ_TEST_POSTGRES_DSN")
		if dsn == "" {
			dsn = "user=postgres password=rootpass sslmode=disable"
		}

		var err error
		db, err = sql.Open("postgres", dsn)
		Expect(err).ShouldNot(HaveOccurred())
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
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return db
		},
	)
})
