package sqlprojection_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/sqltest"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"go.uber.org/multierr"
)

var _ = Describe("type Driver (implementations)", func() {
	Describe("func SelectDriver()", func() {
		It("returns an error if the driver is unrecognized", func() {
			_, err := SelectDriver(context.Background(), unrecognizedDB, BuiltInDrivers())

			expect := "none of the candidate drivers are compatible with sqlprojection_test.fakeDriver"
			for _, e := range multierr.Errors(err) {
				if e.Error() == expect {
					return
				}
			}

			Expect(err).To(MatchError(expect))
		})
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
				})

				AfterEach(func() {
					err := driver.DropSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					err = database.Close()
					Expect(err).ShouldNot(HaveOccurred())

					cancel()
				})

				When("the resource does not exist", func() {
					It("reports an empty version", func() {
						ver, err := driver.QueryVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)

						Expect(err).ShouldNot(HaveOccurred())
						Expect(ver).To(BeEmpty())
					})

					It("stores the version", func() {
						err := driver.StoreVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
							[]byte("<version>"),
						)

						Expect(err).ShouldNot(HaveOccurred())

						ver, err := driver.QueryVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)

						Expect(err).ShouldNot(HaveOccurred())
						Expect(ver).To(Equal([]byte("<version>")))
					})

					table.DescribeTable(
						"it updates the version",
						func(current []byte) {
							tx, err := db.BeginTx(ctx, nil)
							Expect(err).ShouldNot(HaveOccurred())
							defer tx.Rollback()

							ok, err := driver.UpdateVersion(
								ctx,
								tx,
								"<handler>",
								[]byte("<resource>"),
								current,
								[]byte("<version>"),
							)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(ok).To(BeTrue())

							err = tx.Commit()
							Expect(err).ShouldNot(HaveOccurred())

							ver, err := driver.QueryVersion(
								ctx,
								db,
								"<handler>",
								[]byte("<resource>"),
							)

							Expect(err).ShouldNot(HaveOccurred())
							Expect(ver).To(Equal([]byte("<version>")))
						},
						table.Entry("nil byte-slice", nil),
						table.Entry("empty byte-slice", []byte{}),
					)

					It("does not update the version if the supplied current version is incorrect", func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback()

						ok, err := driver.UpdateVersion(
							ctx,
							tx,
							"<handler>",
							[]byte("<resource>"),
							[]byte("<incorrect>"),
							[]byte("<version>"),
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeFalse())
					})
				})

				When("the resource exists", func() {
					BeforeEach(func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback()

						ok, err := driver.UpdateVersion(
							ctx,
							tx,
							"<handler>",
							[]byte("<resource>"),
							nil,
							[]byte("<version>"),
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeTrue())

						err = tx.Commit()
						Expect(err).ShouldNot(HaveOccurred())
					})

					It("reports the expected version", func() {
						ver, err := driver.QueryVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)

						Expect(err).ShouldNot(HaveOccurred())
						Expect(ver).To(Equal([]byte("<version>")))
					})

					It("stores the version", func() {
						err := driver.StoreVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
							[]byte("<version>"),
						)

						Expect(err).ShouldNot(HaveOccurred())

						ver, err := driver.QueryVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)

						Expect(err).ShouldNot(HaveOccurred())
						Expect(ver).To(Equal([]byte("<version>")))
					})

					table.DescribeTable(
						"it updates the version",
						func(next []byte) {
							tx, err := db.BeginTx(ctx, nil)
							Expect(err).ShouldNot(HaveOccurred())
							defer tx.Rollback()

							ok, err := driver.UpdateVersion(
								ctx,
								tx,
								"<handler>",
								[]byte("<resource>"),
								[]byte("<version>"),
								next,
							)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(ok).To(BeTrue())

							err = tx.Commit()
							Expect(err).ShouldNot(HaveOccurred())

							ver, err := driver.QueryVersion(
								ctx,
								db,
								"<handler>",
								[]byte("<resource>"),
							)

							Expect(err).ShouldNot(HaveOccurred())
							Expect(ver).To(Equal(next))
						},
						table.Entry("nil byte-slice", nil),
						table.Entry("empty byte-slice", []byte{}),
						table.Entry("non-empty byte-slice", []byte("<next-version>")),
					)

					table.DescribeTable(
						"it does not update the version if the supplied current version is incorrect",
						func(current []byte) {
							tx, err := db.BeginTx(ctx, nil)
							Expect(err).ShouldNot(HaveOccurred())
							defer tx.Rollback()

							ok, err := driver.UpdateVersion(
								ctx,
								tx,
								"<handler>",
								[]byte("<resource>"),
								current,
								[]byte("<version>"),
							)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(ok).To(BeFalse())
						},
						table.Entry("nil byte-slice", nil),
						table.Entry("empty byte-slice", []byte{}),
						table.Entry("non-empty byte-slice", []byte("<incorrect>")),
					)

					It("can delete the resource", func() {
						err := driver.DeleteResource(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)
						Expect(err).ShouldNot(HaveOccurred())

						ver, err := driver.QueryVersion(
							ctx,
							db,
							"<handler>",
							[]byte("<resource>"),
						)

						Expect(err).ShouldNot(HaveOccurred())
						Expect(ver).To(BeEmpty())
					})
				})
			},
		)
	}
})
