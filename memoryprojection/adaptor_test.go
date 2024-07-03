package memoryprojection_test

import (
	"context"

	"github.com/dogmatiq/dogma"
	// . "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	. "github.com/dogmatiq/projectionkit/memoryprojection"
	"github.com/dogmatiq/projectionkit/memoryprojection/fixtures" // can't dot-import due to conflict
	. "github.com/onsi/ginkgo"
	// . "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var (
		ctx     context.Context
		handler *fixtures.MessageHandler[*int]
		// access  Accessor[*int]
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		handler = &fixtures.MessageHandler[*int]{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		adaptor, _ = New(handler)
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	// Describe("func Configure()", func() {
	// 	It("forwards to the handler", func() {
	// 		Expect(identity.Key(adaptor)).To(Equal("<key>"))
	// 	})
	// })

	// Describe("func HandleEvent()", func() {
	// 	It("forwards to the handler", func() {
	// 		called := false
	// 		handler.HandleEventFunc = func(
	// 			_ *int,
	// 			_ dogma.ProjectionEventScope,
	// 			m dogma.Message,
	// 		) {
	// 			called = true
	// 			Expect(m).To(Equal(MessageA1))
	// 		}

	// 		ok, err := adaptor.HandleEvent(
	// 			ctx,
	// 			[]byte("<resource>"),
	// 			nil,
	// 			[]byte("<version 01>"),
	// 			nil,
	// 			MessageA1,
	// 		)
	// 		Expect(err).ShouldNot(HaveOccurred())
	// 		Expect(ok).To(BeTrue())
	// 		Expect(called).To(BeTrue())
	// 	})
	// })

	// Describe("func TimeoutHint()", func() {
	// 	It("returns zero", func() {
	// 		d := adaptor.TimeoutHint(
	// 			MessageA1,
	// 		)
	// 		Expect(d).To(BeEquivalentTo(0))
	// 	})
	// })

	// Describe("func Compact()", func() {
	// 	It("forwards to the handler", func() {
	// 		called := false
	// 		handler.CompactFunc = func(
	// 			_ *int,
	// 			_ dogma.ProjectionCompactScope,
	// 		) {
	// 			called = true
	// 		}

	// 		err := adaptor.Compact(
	// 			context.Background(),
	// 			nil, // scope
	// 		)
	// 		Expect(err).ShouldNot(HaveOccurred())
	// 		Expect(called).To(BeTrue())
	// 	})
	// })
})
