package postgres_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/postgres"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	products := []drivertest.Product{
		drivertest.PostgreSQL,
		drivertest.CockroachDB,
	}

	drivers := []string{
		"postgres",
		"pgx",
	}

	for _, product := range products {
		for _, driver := range drivers {
			product := product // capture loop variable
			driver := driver   // capture loop variable

			When(
				fmt.Sprintf(
					"using the '%s' driver with %s",
					driver,
					product,
				),
				func() {
					var (
						db      *sql.DB
						closeDB func()
					)

					BeforeEach(func() {
						db, _, closeDB = drivertest.Open(product, driver)
					})

					AfterEach(func() {
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
				},
			)
		}
	}
})
