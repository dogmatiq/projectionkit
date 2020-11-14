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
		driver  string
		db      *sql.DB
		closeDB func()
	)

	JustBeforeEach(func() {
		db, _, closeDB = drivertest.Open(driver)
	})

	AfterEach(func() {
		if closeDB != nil {
			closeDB()
		}
	})

	When("using the 'postgres' driver", func() {
		BeforeEach(func() {
			driver = "postgres"
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

	When("using the 'pgx' driver", func() {
		BeforeEach(func() {
			driver = "pgx"
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
})
