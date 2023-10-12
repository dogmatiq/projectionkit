package syncprojection_test

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/syncprojection"
	"github.com/dogmatiq/projectionkit/syncprojection/fixtures" // can't dot-import due to conflict
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	type state struct {
		IsReady bool
	}

	var (
		ctx       context.Context
		handler   *fixtures.MessageHandler[*state]
		awaitable Awaitable[*state]
		adaptor   dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		handler = &fixtures.MessageHandler[*state]{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "caa87889-4734-44ba-adcf-ffb1ae4d0ddb")
		}

		awaitable, adaptor = New(handler)
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(adaptor)).To(Equal("caa87889-4734-44ba-adcf-ffb1ae4d0ddb"))
		})
	})

	Describe("func HandleEvent()", func() {
		err := awaitable.Await(
			ctx,
			"<instance>",
			func(s *state) bool {
				return s.IsReady
			},
		)
		Expect(err).ShouldNot(HaveOccurred())
	})
})
