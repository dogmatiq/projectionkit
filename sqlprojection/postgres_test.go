package sqlprojection_test

import (
	"context"
	"database/sql"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("var PostgresDriver", func() {
	var (
		driver  string
		db      *sql.DB
		closeDB func()
	)

	JustBeforeEach(func() {
		db, _, closeDB = drivertest.Open(drivertest.PostgreSQL, driver)
	})

	AfterEach(func() {
		if closeDB != nil {
			closeDB()
		}
	})

	setup := func(ctx context.Context) *sql.DB {
		err := PostgresDriver.DropSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		err = PostgresDriver.CreateSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		return db
	}

	When("using the 'postgres' driver", func() {
		BeforeEach(func() {
			driver = "postgres"
		})

		drivertest.Declare(PostgresDriver, setup)
	})

	When("using the 'pgx' driver", func() {
		BeforeEach(func() {
			driver = "pgx"
		})

		drivertest.Declare(PostgresDriver, setup)
	})
})
