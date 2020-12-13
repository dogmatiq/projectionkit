package sqlprojection_test

import (
	"context"
	"database/sql"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/sqltest/sqlstub"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/multierr"
)

var _ = Describe("func SelectDriver()", func() {
	It("returns an error if the driver is unrecognized", func() {
		_, err := SelectDriver(
			context.Background(),
			sql.OpenDB(&sqlstub.Connector{}),
			BuiltInDrivers(),
		)

		expect := "could not find a driver that is compatible with *sqlstub.Driver"
		for _, e := range multierr.Errors(err) {
			if e.Error() == expect {
				return
			}
		}

		Expect(err).To(MatchError(expect))
	})
})
