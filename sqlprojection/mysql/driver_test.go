package mysql_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sqlprojection/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
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
})
