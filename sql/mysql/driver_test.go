package mysql_test

import (
	"context"
	"database/sql"
	"os"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/mysql"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var db *sql.DB

	BeforeSuite(func() {
		dsn := os.Getenv("DOGMATIQ_TEST_MYSQL_DSN")
		if dsn == "" {
			dsn = "root:rootpass@tcp(127.0.0.1:3306)/dogmatiq"
		}

		var err error
		db, err = sql.Open("mysql", dsn)
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
