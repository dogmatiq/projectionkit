package memoryprojection_test

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/memoryprojection"
	. "github.com/dogmatiq/projectionkit/memoryprojection"
	"github.com/dogmatiq/projectionkit/memoryprojection/internal/fixtures" // can't dot-import due to conflict
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Projection", func() {
	var (
		ctx        context.Context
		handler    *fixtures.MessageHandler[int]
		projection *Projection[int, *fixtures.MessageHandler[int]]
	)

	BeforeEach(func() {
		ctx = context.Background()

		handler = &fixtures.MessageHandler[int]{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		projection = &Projection[int, *fixtures.MessageHandler[int]]{
			Handler: handler,
		}
	})

	adaptortest.DescribeAdaptor(&ctx, &projection)

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(projection)).To(Equal("<key>"))
		})
	})

	When("there is no state", func() {
		Describe("func HandleEvent()", func() {
			It("forwards a zero value to the handler", func() {
				called := false
				handler.HandleEventFunc = func(
					v int,
					_ dogma.ProjectionEventScope,
					m dogma.Event,
				) (int, error) {
					called = true
					Expect(v).To(Equal(0))
					Expect(m).To(Equal(EventA1))
					return v, nil
				}

				cp, err := projection.HandleEvent(
					ctx,
					&stubs.ProjectionEventScopeStub{},
					EventA1,
				)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(cp).To(BeEquivalentTo(1))
				Expect(called).To(BeTrue())
			})
		})

		Describe("func Compact()", func() {
			It("does not forward to the handler", func() {
				handler.CompactFunc = func(
					int,
					dogma.ProjectionCompactScope,
				) int {
					Fail("unexpected call")
					return 0
				}

				err := projection.Compact(
					context.Background(),
					nil, // scope
				)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Describe("func Query()", func() {
			It("calls the query function with a zero value", func() {
				r := memoryprojection.Query(
					projection,
					func(v int) int {
						return v * 2
					},
				)
				Expect(r).To(Equal(0))
			})
		})
	})

	When("there is existing state", func() {
		BeforeEach(func() {
			handler.HandleEventFunc = func(
				v int,
				_ dogma.ProjectionEventScope,
				_ dogma.Event,
			) (int, error) {
				return 321, nil
			}

			cp, err := projection.HandleEvent(
				ctx,
				&stubs.ProjectionEventScopeStub{},
				EventA1,
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(cp).To(BeEquivalentTo(1))
		})

		Describe("func HandleEvent()", func() {
			It("forwards the existing value to the handler", func() {
				called := false
				handler.HandleEventFunc = func(
					v int,
					_ dogma.ProjectionEventScope,
					m dogma.Event,
				) (int, error) {
					called = true
					Expect(v).To(Equal(321))
					return v, nil
				}

				cp, err := projection.HandleEvent(
					ctx,
					&stubs.ProjectionEventScopeStub{
						OffsetFunc:           func() uint64 { return 1 },
						CheckpointOffsetFunc: func() uint64 { return 1 },
					},
					EventA1,
				)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(cp).To(BeEquivalentTo(2))
				Expect(called).To(BeTrue())
			})
		})

		Describe("func Compact()", func() {
			It("forwards to the handler", func() {
				called := false
				handler.CompactFunc = func(
					v int,
					_ dogma.ProjectionCompactScope,
				) int {
					called = true
					Expect(v).To(Equal(321))
					return v
				}

				err := projection.Compact(
					context.Background(),
					nil, // scope
				)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue())
			})
		})

		Describe("func Query()", func() {
			It("calls the query function with the existing value", func() {
				r := memoryprojection.Query(
					projection,
					func(v int) int {
						return v * 2
					},
				)
				Expect(r).To(Equal(642))
			})
		})
	})
})
