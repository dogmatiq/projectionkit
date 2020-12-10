package sqlite_test

import (
	"database/sql"

	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sqlprojection/sqlite"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func IsCompatibleWith()", func() {
	It("returns true if the driver is recognized", func() {
		db, err := sql.Open("sqlite3", ":memory:")
		Expect(err).ShouldNot(HaveOccurred())
		Expect(IsCompatibleWith(db)).To(BeTrue())
	})

	It("returns false if the driver is unrecognized", func() {
		Expect(IsCompatibleWith(drivertest.MockDB())).To(BeFalse())
	})
})
