package sqlprojection_test

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("var MySQLDriver", func() {
	products := []drivertest.Product{
		drivertest.MySQL,
		drivertest.MariaDB,
	}

	for _, product := range products {
		product := product // capture loop variable

		When(
			fmt.Sprintf(
				"using the 'mysql' driver with %s",
				product,
			),
			func() {
				var (
					db      *sql.DB
					closeDB func()
				)

				BeforeEach(func() {
					db, _, closeDB = drivertest.Open(product, "mysql")
				})

				AfterEach(func() {
					if closeDB != nil {
						closeDB()
					}
				})

				drivertest.Declare(
					MySQLDriver,
					func(ctx context.Context) *sql.DB {
						err := MySQLDriver.DropSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())

						err = MySQLDriver.CreateSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())

						return db
					},
				)
			},
		)
	}
})
