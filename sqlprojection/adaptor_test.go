package sqlprojection_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/sqltest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var handler *fixtures.MessageHandler

	BeforeEach(func() {
		handler = &fixtures.MessageHandler{}
	})

	for _, pair := range sqltest.CompatiblePairs {
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
					driver   Driver
					db       *sql.DB
					adaptor  dogma.ProjectionMessageHandler
				)

				BeforeEach(func() {
					ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

					var err error
					database, err = sqltest.NewDatabase(ctx, pair.Driver, pair.Product)
					Expect(err).ShouldNot(HaveOccurred())

					db, err = database.Open()
					Expect(err).ShouldNot(HaveOccurred())

					driver, err = SelectDriver(ctx, db, BuiltInDrivers())
					Expect(err).ShouldNot(HaveOccurred())

					err = driver.CreateSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					adaptor = New(db, handler)
				})

				AfterEach(func() {
					err := driver.DropSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					err = database.Close()
					Expect(err).ShouldNot(HaveOccurred())

					cancel()
				})

				adaptortest.Declare(
					func(ctx context.Context) dogma.ProjectionMessageHandler {
						return adaptor
					},
				)

				Describe("func Compact()", func() {
					It("forwards to the handler", func() {
						handler.CompactFunc = func(
							_ context.Context,
							d *sql.DB,
							_ dogma.ProjectionCompactScope,
						) error {
							Expect(d).To(BeIdenticalTo(db))
							return errors.New("<error>")
						}

						err := adaptor.Compact(
							context.Background(),
							nil, // scope
						)
						Expect(err).To(MatchError("<error>"))
					})
				})
			},
		)
	}

	Describe("func New()", func() {
		It("returns an unbound handler if the database is nil", func() {
			adaptor := New(nil, handler)

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})
})
