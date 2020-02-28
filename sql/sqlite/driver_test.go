package sqlite_test

import (
	"context"
	"database/sql"
	"strings"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var db *sql.DB

	BeforeSuite(func() {
		db = drivertest.Open("sqlite3")
	})

	AfterSuite(func() {
		if db != nil {
			db.Close()
		}
	})

	drivertest.Declare(
		&Driver{},
		func(ctx context.Context) *sql.DB {
			err := DropSchema(ctx, db)
			if err != nil && strings.Contains(err.Error(), "CGO_ENABLED") {
				Skip(err.Error())
			}
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return db
		},
	)
})
