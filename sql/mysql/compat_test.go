package mysql_test

import (
	"database/sql"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/mysql"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func IsCompatibleWith()", func() {
	It("returns true if the driver is recognized", func() {
		db, err := sql.Open("mysql", "tcp(127.0.0.1)/mysql")
		Expect(err).ShouldNot(HaveOccurred())
		Expect(IsCompatibleWith(db)).To(BeTrue())
	})

	It("returns false if the driver is unrecognized", func() {
		Expect(IsCompatibleWith(drivertest.MockDB())).To(BeFalse())
	})
})
