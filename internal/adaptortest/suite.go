package adaptortest

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/resource"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// Declare decalres generic behavioral tests for a specific adaptor
// implementation.
func Declare(setup func(context.Context) dogma.ProjectionMessageHandler) {
	var (
		ctx     context.Context
		cancel  func()
		adaptor dogma.ProjectionMessageHandler
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel = context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		adaptor = setup(ctx)
	})

	ginkgo.AfterEach(func() {
		cancel()
	})

	ginkgo.Context("low-level resource API", func() {
		ginkgo.When("the resource does not exist", func() {
			ginkgo.It("stores the version", func() {
				err := resource.StoreVersion(
					ctx,
					adaptor,
					[]byte("<resource>"),
					[]byte("<version>"),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				ver, err := adaptor.ResourceVersion(
					ctx,
					[]byte("<resource>"),
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
			})

			table.DescribeTable(
				"it updates the version",
				func(current []byte) {
					ok, err := resource.UpdateVersion(
						ctx,
						adaptor,
						[]byte("<resource>"),
						current,
						[]byte("<version>"),
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ok).To(gomega.BeTrue())

					ver, err := adaptor.ResourceVersion(
						ctx,
						[]byte("<resource>"),
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
				},
				table.Entry("nil byte-slice", nil),
				table.Entry("empty byte-slice", []byte{}),
			)

			ginkgo.It("does not update the version if the supplied current version is incorrect", func() {
				ok, err := resource.UpdateVersion(
					ctx,
					adaptor,
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
				err := resource.StoreVersion(
					ctx,
					adaptor,
					[]byte("<resource>"),
					[]byte("<version>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})

			ginkgo.It("reports the expected version", func() {
				ver, err := adaptor.ResourceVersion(
					ctx,
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
			})

			ginkgo.It("stores the version", func() {
				err := resource.StoreVersion(
					ctx,
					adaptor,
					[]byte("<resource>"),
					[]byte("<version>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				ver, err := adaptor.ResourceVersion(
					ctx,
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.Equal([]byte("<version>")))
			})

			table.DescribeTable(
				"it stores an empty version",
				func(next []byte) {
					err := resource.StoreVersion(
						ctx,
						adaptor,
						[]byte("<resource>"),
						next,
					)

					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ver, err := adaptor.ResourceVersion(
						ctx,
						[]byte("<resource>"),
					)

					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ver).To(gomega.BeEmpty())
				},
				table.Entry("nil byte-slice", nil),
				table.Entry("empty byte-slice", []byte{}),
			)

			table.DescribeTable(
				"it updates the version",
				func(next []byte) {
					ok, err := resource.UpdateVersion(
						ctx,
						adaptor,
						[]byte("<resource>"),
						[]byte("<version>"),
						next,
					)

					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ok).To(gomega.BeTrue())

					ver, err := adaptor.ResourceVersion(
						ctx,
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
					ok, err := resource.UpdateVersion(
						ctx,
						adaptor,
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
				err := resource.DeleteResource(
					ctx,
					adaptor,
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				ver, err := adaptor.ResourceVersion(
					ctx,
					[]byte("<resource>"),
				)

				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ver).To(gomega.BeEmpty())
			})
		})
	})
}
