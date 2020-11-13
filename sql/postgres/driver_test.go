package postgres_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/postgres"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var (
		db      *sql.DB
		closeDB func()
	)

	BeforeSuite(func() {
		db, _, closeDB = drivertest.Open("postgres")
	})

	AfterSuite(func() {
		if closeDB != nil {
			closeDB()
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
