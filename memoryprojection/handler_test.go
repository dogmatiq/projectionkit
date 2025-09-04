package memoryprojection_test

import (
	. "github.com/dogmatiq/projectionkit/memoryprojection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NoCompactBehavior", func() {
	When("Compact() is called", func() {
		It("returns nil", func() {
			var v NoCompactBehavior[any]

			err := v.Compact(nil, nil)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
