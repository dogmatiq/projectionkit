package sqlprojection_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/deslittle/projectionkit/internal/adaptortest"
	"github.com/deslittle/projectionkit/internal/identity"
	. "github.com/deslittle/projectionkit/sqlprojection"
	"github.com/deslittle/projectionkit/sqlprojection/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/sqltest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var handler *fixtures.MessageHandler

	BeforeEach(func() {
		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}
	})

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
					adaptor  dogma.ProjectionMessageHandler
				)

				BeforeEach(func() {
					ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

					var err error
					database, err = sqltest.NewDatabase(ctx, pair.Driver, pair.Product)
					Expect(err).ShouldNot(HaveOccurred())

					db, err = database.Open()
					Expect(err).ShouldNot(HaveOccurred())

					err = CreateSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					adaptor = New(db, handler)
				})

				AfterEach(func() {
					err := DropSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					err = database.Close()
					Expect(err).ShouldNot(HaveOccurred())

					cancel()
				})

				adaptortest.DescribeAdaptor(&ctx, &adaptor)

				Describe("func Configure()", func() {
					It("forwards to the handler", func() {
						Expect(identity.Key(adaptor)).To(Equal("<key>"))
					})
				})

				Describe("func HandleEvent()", func() {
					It("returns an error if the application's message handler fails", func() {
						terr := errors.New("handle event test error")

						handler.HandleEventFunc = func(
							context.Context,
							*sql.Tx,
							dogma.ProjectionEventScope,
							dogma.Message,
						) error {
							return terr
						}

						_, err := adaptor.HandleEvent(
							context.Background(),
							[]byte("<resource>"),
							nil,
							[]byte("<version 01>"),
							nil,
							MessageA1,
						)
						Expect(err).Should(HaveOccurred())
					})
				})

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
