package adaptortest

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DescribeAdaptor declares generic behavioral tests for a specific adaptor
// implementation.
func DescribeAdaptor[T dogma.ProjectionMessageHandler](
	ctxP *context.Context,
	adaptorP *T,
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
		ginkgo.It("returns the new checkpoint offset", func() {
			ginkgo.By("applying the event at offset 0")

			cp, err := adaptor.HandleEvent(
				ctx,
				&stubs.ProjectionEventScopeStub{},
				stubs.EventA1,
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cp).Should(gomega.BeEquivalentTo(1))

			ginkgo.By("applying the event at offset 1")

			cp, err = adaptor.HandleEvent(
				ctx,
				&stubs.ProjectionEventScopeStub{
					OffsetFunc:           func() uint64 { return 1 },
					CheckpointOffsetFunc: func() uint64 { return 1 },
				},
				stubs.EventA2,
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cp).Should(gomega.BeEquivalentTo(2))
		})

		ginkgo.It("returns the actual checkpoint offset if the provided checkpoint offset is not current", func() {
			cp, err := adaptor.HandleEvent(
				ctx,
				&stubs.ProjectionEventScopeStub{},
				stubs.EventA1,
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cp).Should(gomega.BeEquivalentTo(1))

			cp, err = adaptor.HandleEvent(
				ctx,
				&stubs.ProjectionEventScopeStub{
					CheckpointOffsetFunc: func() uint64 { return 123 },
				},
				stubs.EventA2,
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cp).Should(gomega.BeEquivalentTo(1))
		})
	})

	ginkgo.Describe("func CheckpointOffset()", func() {
		ginkgo.It("returns the checkpoint offset", func() {
			s := &stubs.ProjectionEventScopeStub{}

			want, err := adaptor.HandleEvent(
				ctx,
				s,
				stubs.EventA1,
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(want).Should(gomega.BeEquivalentTo(1))

			got, err := adaptor.CheckpointOffset(
				ctx,
				s.StreamID(),
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(got).To(gomega.Equal(want))
		})

		ginkgo.It("returns 0 if no events from the stream have been applied", func() {
			cp, err := adaptor.CheckpointOffset(
				ctx,
				"e108b1d5-f2c2-44f1-884d-a5cdc1d575f0",
			)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(cp).To(gomega.BeZero())
		})
	})
}
