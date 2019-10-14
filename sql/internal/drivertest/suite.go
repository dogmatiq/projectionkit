package drivertest

import (
	"context"
	"database/sql"
	"time"

	pksql "github.com/dogmatiq/projectionkit/sql"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// Declare decalres generic behavioral tests for a specific driver
// implementation.
func Declare(
	d pksql.Driver,
	setup func(context.Context) *sql.DB,
) {
	var (
		db     *sql.DB
		ctx    context.Context
		cancel func()
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel = context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		db = setup(ctx)
	})

	ginkgo.AfterEach(func() {
		cancel()
	})

	ginkgo.When("the resource does not exist", func() {
		ginkgo.It("reports an empty version", func() {
			ver, err := d.QueryVersion(
				ctx,
				db,
				"<handler>",
				[]byte("<resource>"),
			)

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ver).To(gomega.BeEmpty())
		})

		table.DescribeTable(
			"it updates the version",
			func(current []byte) {
				tx, err := db.BeginTx(ctx, nil)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx.Rollback()

				ok, err := d.UpdateVersion(
					ctx,
					tx,
					"<handler>",
					[]byte("<resource>"),
					current,
					[]byte("<version>"),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ok).To(gomega.BeTrue())

				err = tx.Commit()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				ver, err := d.QueryVersion(
					ctx,
					db,
					"<handler>",
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
			},
			table.Entry("nil byte-slice", nil),
			table.Entry("empty byte-slice", []byte{}),
		)

		ginkgo.It("does not update the version if the supplied current version is incorrect", func() {
			tx, err := db.BeginTx(ctx, nil)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			defer tx.Rollback()

			ok, err := d.UpdateVersion(
				ctx,
				tx,
				"<handler>",
				[]byte("<resource>"),
				[]byte("<incorrect>"),
				[]byte("<version>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ok).To(gomega.BeFalse())
		})
	})

	ginkgo.When("the resource exists", func() {
		ginkgo.BeforeEach(func() {
			tx, err := db.BeginTx(ctx, nil)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			defer tx.Rollback()

			ok, err := d.UpdateVersion(
				ctx,
				tx,
				"<handler>",
				[]byte("<resource>"),
				nil,
				[]byte("<version>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ok).To(gomega.BeTrue())

			err = tx.Commit()
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("reports the expected version", func() {
			ver, err := d.QueryVersion(
				ctx,
				db,
				"<handler>",
				[]byte("<resource>"),
			)

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
		})

		table.DescribeTable(
			"it updates the version",
			func(next []byte) {
				tx, err := db.BeginTx(ctx, nil)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx.Rollback()

				ok, err := d.UpdateVersion(
					ctx,
					tx,
					"<handler>",
					[]byte("<resource>"),
					[]byte("<version>"),
					next,
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ok).To(gomega.BeTrue())

				err = tx.Commit()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				ver, err := d.QueryVersion(
					ctx,
					db,
					"<handler>",
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.Equal(next))
			},
			table.Entry("nil byte-slice", nil),
			table.Entry("empty byte-slice", []byte{}),
			table.Entry("non-empty byte-slice", []byte("<next-version>")),
		)

		table.DescribeTable(
			"it does not update the version if the supplied current version is incorrect",
			func(current []byte) {
				tx, err := db.BeginTx(ctx, nil)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx.Rollback()

				ok, err := d.UpdateVersion(
					ctx,
					tx,
					"<handler>",
					[]byte("<resource>"),
					current,
					[]byte("<version>"),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ok).To(gomega.BeFalse())
			},
			table.Entry("nil byte-slice", nil),
			table.Entry("empty byte-slice", []byte{}),
			table.Entry("non-empty byte-slice", []byte("<incorrect>")),
		)

		ginkgo.It("can delete the resource", func() {
			err := d.DeleteResource(
				ctx,
				db,
				"<handler>",
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			ver, err := d.QueryVersion(
				ctx,
				db,
				"<handler>",
				[]byte("<resource>"),
			)

			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(ver).To(gomega.BeEmpty())
		})
	})
}
