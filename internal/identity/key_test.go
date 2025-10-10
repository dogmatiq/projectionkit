package identity_test

import (
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Key()", func() {
	It("returns the identity key of the handler", func() {
		id := uuidpb.Generate()

		h := &ProjectionMessageHandlerStub{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<name>", id.AsString())
				c.Routes(
					dogma.HandlesEvent[*EventStub[TypeA]](),
				)
			},
		}

		Expect(Key(h).Equal(id)).To(BeTrue())
	})
})
