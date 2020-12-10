package sqlprojection_test

import (
	"database/sql"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	"github.com/dogmatiq/projectionkit/sqlprojection/mysql"
	"github.com/dogmatiq/projectionkit/sqlprojection/postgres"
	"github.com/dogmatiq/projectionkit/sqlprojection/sqlite"
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
		func(name, dsn string, expected Driver) {
			db, err := sql.Open(name, dsn)
			Expect(err).ShouldNot(HaveOccurred())
			defer db.Close()

			d, err := NewDriver(db)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(d).To(Equal(expected))
		},
		Entry("mysql", "mysql", "tcp(127.0.0.1)/mysql", &mysql.Driver{}),
		Entry("postgres", "postgres", "host=localhost", &postgres.Driver{}),
		Entry("pgx", "pgx", "postgres://localhost", &postgres.Driver{}),
		Entry("sqlite", "sqlite3", ":memory:", &sqlite.Driver{}),
	)

	It("returns an error if the driver is unrecognised", func() {
		_, err := NewDriver(drivertest.MockDB())
		Expect(err).To(MatchError("can not deduce the appropriate SQL projection driver for *drivertest.MockDriver"))
	})
})
