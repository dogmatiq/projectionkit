package unboundhandler_test

import (
	"context"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/internal/unboundhandler"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Handler", func() {
	var (
		upstream *ProjectionMessageHandlerStub
		handler  dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		upstream = &ProjectionMessageHandlerStub{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<name>", "<key>")
			},
		}
		handler = New(upstream)
	})

	Describe("func Configure()", func() {
		It("forwards to the upstream handler", func() {
			k := identity.Key(handler)
			Expect(k).To(Equal("<key>"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("returns an error", func() {
			_, err := handler.HandleEvent(context.Background(), nil, nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func CheckpointOffset()", func() {
		It("returns an error", func() {
			_, err := handler.CheckpointOffset(context.Background(), "18819292-dbe7-4a89-b3e7-0aeb46939fa9")
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func Compact()", func() {
		It("returns an error", func() {
			err := handler.Compact(context.Background(), nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})
})
