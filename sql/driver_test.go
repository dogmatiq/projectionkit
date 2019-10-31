package sql_test

import (
	"database/sql"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"

	pksql "github.com/dogmatiq/projectionkit/sql"
	"github.com/dogmatiq/projectionkit/sql/mysql"
	"github.com/dogmatiq/projectionkit/sql/postgres"
	"github.com/dogmatiq/projectionkit/sql/sqlite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("func NewDriver()", func() {
	DescribeTable(
		"it returns the expected driver",
		func(name, dsn string, expected pksql.Driver) {
			db, err := sql.Open(name, dsn)
			Expect(err).ShouldNot(HaveOccurred())
			defer db.Close()

			d, err := pksql.NewDriver(db)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(d).To(Equal(expected))
		},
		Entry("mysql", "mysql", "localhost", &mysql.Driver{}),
		Entry("postgres", "postgres", "host=localhost", &postgres.Driver{}),
		Entry("sqlite", "sqlite3", ":memory:", &sqlite.Driver{}),
	)

	It("returns an error if the driver is unrecognised", func() {
		_, err := pksql.NewDriver(drivertest.MockDB())
		Expect(err).Should(HaveOccurred())
	})
})
