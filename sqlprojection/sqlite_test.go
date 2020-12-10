package sqlprojection_test

import (
	"context"
	"database/sql"
	"strings"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/internal/drivertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("var SQLiteDriver", func() {
	var (
		db      *sql.DB
		closeDB func()
	)

	BeforeEach(func() {
		db, _, closeDB = drivertest.Open(drivertest.SQLite, "sqlite3")
	})

	AfterEach(func() {
		if closeDB != nil {
			closeDB()
		}
	})

	drivertest.Declare(
		SQLiteDriver,
		func(ctx context.Context) *sql.DB {
			err := SQLiteDriver.DropSchema(ctx, db)
			if err != nil && strings.Contains(err.Error(), "CGO_ENABLED") {
				Skip(err.Error())
			}
			Expect(err).ShouldNot(HaveOccurred())

			err = SQLiteDriver.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return db
		},
	)
})
