package sqlprojection_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/sqltest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("creating and dropping schema", func() {
	for _, pair := range sqltest.CompatiblePairs() {
		pair := pair // capture loop variable

		When(
			fmt.Sprintf(
				"using %s with the '%s' driver",
				pair.Product.Name(),
				pair.Driver.Name(),
			),
			func() {
				var (
					ctx      context.Context
					cancel   context.CancelFunc
					database *sqltest.Database
					db       *sql.DB
				)

				BeforeEach(func() {
					ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

					var err error
					database, err = sqltest.NewDatabase(ctx, pair.Driver, pair.Product)
					Expect(err).ShouldNot(HaveOccurred())

					db, err = database.Open()
					Expect(err).ShouldNot(HaveOccurred())
				})

				AfterEach(func() {
					err := database.Close()
					Expect(err).ShouldNot(HaveOccurred())

					cancel()
				})

				Describe("func CreateSchema()", func() {
					It("can be called when the schema already exists", func() {
						err := CreateSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())

						err = CreateSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})

				Describe("func DropSchema()", func() {
					It("can be called when the schema does not exist", func() {
						err := DropSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())
					})

					It("can be called when the schema has already been dropped", func() {
						err := CreateSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())

						err = DropSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())

						err = DropSchema(ctx, db)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})
			},
		)
	}
})
