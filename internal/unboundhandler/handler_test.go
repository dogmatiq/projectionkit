package unboundhandler_test

import (
	"context"

	"github.com/deslittle/projectionkit/internal/identity"
	. "github.com/deslittle/projectionkit/internal/unboundhandler"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Handler", func() {
	var (
		upstream *ProjectionMessageHandler
		handler  dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		upstream = &ProjectionMessageHandler{
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
			_, err := handler.HandleEvent(context.Background(), nil, nil, nil, nil, nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func ResourceVersion()", func() {
		It("returns an error", func() {
			_, err := handler.ResourceVersion(context.Background(), nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func CloseResource()", func() {
		It("returns an error", func() {
			err := handler.CloseResource(context.Background(), nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func TimeoutHint()", func() {
		It("returns 0", func() {
			d := handler.TimeoutHint(MessageA1)
			Expect(d).To(BeZero())
		})
	})

	Describe("func Compact()", func() {
		It("returns an error", func() {
			err := handler.Compact(context.Background(), nil)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})
})
