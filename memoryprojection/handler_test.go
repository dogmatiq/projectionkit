package memoryprojection_test

import (
	"context"

	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NoCompactBehavior", func() {
	When("Compact() is called", func() {
		It("returns nil", func() {
			var v NoCompactBehavior

			err := v.Compact(context.Background(), nil, nil)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
