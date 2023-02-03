package identity_test

import (
	. "github.com/deslittle/projectionkit/internal/identity"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Key()", func() {
	It("returns the identity key of the handler", func() {
		h := &ProjectionMessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<name>", "<key>")
				c.ConsumesEventType(MessageA{})
			},
		}

		Expect(Key(h)).To(Equal("<key>"))
	})
})
