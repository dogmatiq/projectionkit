package sqlprojection_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/sqltest"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func selectDriver(p sqltest.Pair) (Driver, error) {
	if p.Product == sqltest.SQLite {
		return SQLiteDriver, nil
	}

	switch prod := p.Product.(type) {
	case sqltest.MySQLCompatibleProduct:
		return MySQLDriver, nil
	case sqltest.PostgresCompatibleProduct:
		return PostgresDriver, nil
	default:
		return nil, fmt.Errorf("unsupported product: %s", prod.Name())
	}
}

var _ = Describe("type Driver (implementations)", func() {
	for _, pair := range sqltest.CompatiblePairs() {
		When(
			fmt.Sprintf(
				"using %s with the '%s' driver",
				pair.Product.Name(),
				pair.Driver.Name(),
			),
			func() {
				var (
					ctx        context.Context
					cancel     context.CancelFunc
					database   *sqltest.Database
					driver     Driver
					db         *sql.DB
					handlerKey *uuidpb.UUID
					streamID   *uuidpb.UUID
				)

				BeforeEach(func() {
					ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

					var err error
					database, err = sqltest.NewDatabase(ctx, pair.Driver, pair.Product)
					Expect(err).ShouldNot(HaveOccurred())

					db, err = database.Open()
					Expect(err).ShouldNot(HaveOccurred())

					driver, err = selectDriver(pair)
					Expect(err).ShouldNot(HaveOccurred())

					err = driver.CreateSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					handlerKey = uuidpb.Generate()
					streamID = uuidpb.Generate()
				})

				AfterEach(func() {
					err := driver.DropSchema(ctx, db)
					Expect(err).ShouldNot(HaveOccurred())

					err = database.Close()
					Expect(err).ShouldNot(HaveOccurred())

					cancel()
				})

				When("no checkpoint offset has been stored", func() {
					It("reports the zero offset", func() {
						cp, err := driver.QueryCheckpointOffset(
							ctx,
							db,
							handlerKey,
							streamID,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(cp).To(BeZero())
					})

					It("updates the checkpoint offset", func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback() // nolint:errcheck

						ok, err := driver.UpdateCheckpointOffset(
							ctx,
							tx,
							handlerKey,
							streamID,
							0,
							123,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeTrue())

						err = tx.Commit()
						Expect(err).ShouldNot(HaveOccurred())

						cp, err := driver.QueryCheckpointOffset(
							ctx,
							db,
							handlerKey,
							streamID,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(cp).To(BeEquivalentTo(123))
					})

					It("does not update the checkpoint offset if the supplied 'current' offset is incorrect", func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback() // nolint:errcheck

						ok, err := driver.UpdateCheckpointOffset(
							ctx,
							tx,
							handlerKey,
							streamID,
							123,
							123,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeFalse())

						err = tx.Commit()
						Expect(err).ShouldNot(HaveOccurred())

						cp, err := driver.QueryCheckpointOffset(
							ctx,
							db,
							handlerKey,
							streamID,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(cp).To(BeZero())
					})
				})

				When("a checkpoint offset has been stored", func() {
					BeforeEach(func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback()

						ok, err := driver.UpdateCheckpointOffset(
							ctx,
							tx,
							handlerKey,
							streamID,
							0,
							123,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeTrue())

						err = tx.Commit()
						Expect(err).ShouldNot(HaveOccurred())
					})

					It("reports the stored checkpoint offset", func() {
						cp, err := driver.QueryCheckpointOffset(
							ctx,
							db,
							handlerKey,
							streamID,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(cp).To(BeEquivalentTo(123))
					})

					It("updates the checkpoint offset", func() {
						tx, err := db.BeginTx(ctx, nil)
						Expect(err).ShouldNot(HaveOccurred())
						defer tx.Rollback()

						ok, err := driver.UpdateCheckpointOffset(
							ctx,
							tx,
							handlerKey,
							streamID,
							123,
							456,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(ok).To(BeTrue())

						err = tx.Commit()
						Expect(err).ShouldNot(HaveOccurred())

						cp, err := driver.QueryCheckpointOffset(
							ctx,
							db,
							handlerKey,
							streamID,
						)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(cp).To(BeEquivalentTo(456))
					})

					table.DescribeTable(
						"it does not update the checkpoint offset if the supplied 'current' offset is incorrect",
						func(current int) {
							tx, err := db.BeginTx(ctx, nil)
							Expect(err).ShouldNot(HaveOccurred())
							defer tx.Rollback()

							ok, err := driver.UpdateCheckpointOffset(
								ctx,
								tx,
								handlerKey,
								streamID,
								uint64(current),
								456,
							)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(ok).To(BeFalse())

							err = tx.Commit()
							Expect(err).ShouldNot(HaveOccurred())

							cp, err := driver.QueryCheckpointOffset(
								ctx,
								db,
								handlerKey,
								streamID,
							)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(cp).To(BeEquivalentTo(123))
						},
						table.Entry("zero", 0),
						table.Entry("less than actual", 122),
						table.Entry("greater than actual", 124),
						table.Entry("same as new value", 456),
					)
				})
			},
		)
	}
})
