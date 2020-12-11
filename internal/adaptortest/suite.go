package adaptortest

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/projectionkit/resource"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DescribeAdaptor declares generic behavioral tests for a specific adaptor
// implementation.
func DescribeAdaptor(
	ctxP *context.Context,
	adaptorP *dogma.ProjectionMessageHandler,
) {
	var (
		ctx     context.Context
		adaptor dogma.ProjectionMessageHandler
	)

	ginkgo.BeforeEach(func() {
		ctx = *ctxP
		adaptor = *adaptorP
	})

	ginkgo.Describe("func HandleEvent()", func() {
		ginkgo.It("does not produce errors when OCC parameters are supplied correctly", func() {
			ginkgo.By("persisting the initial resource version")

			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.Equal([]byte("<version 01>")))

			ginkgo.By("persisting the next resource version")

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<version 01>"),
				[]byte("<version 02>"),
				nil,
				fixtures.MessageA2,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			v, err = adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.Equal([]byte("<version 02>")))

			ginkgo.By("discarding a resource if the next resource version is empty")

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<version 02>"),
				nil,
				nil,
				fixtures.MessageA3,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			v, err = adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.BeEmpty())
		})

		ginkgo.It("returns false if supplied resource version is not the current version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<incorrect current version>"),
				[]byte("<version 02>"),
				nil,
				fixtures.MessageA2,
			)
			gomega.Expect(ok).Should(gomega.BeFalse())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})
	})

	ginkgo.Describe("func ResourceVersion()", func() {
		ginkgo.It("returns a resource version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.Equal([]byte("<version 01>")))
		})

		ginkgo.It("returns nil if no current resource version present in the database", func() {
			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.BeEmpty())
		})
	})

	ginkgo.Describe("func CloseResource()", func() {
		ginkgo.It("removes a resource version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA2,
			)
			gomega.Expect(ok).Should(gomega.BeTrue())
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			err = adaptor.CloseResource(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(v).To(gomega.BeEmpty())
		})
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
