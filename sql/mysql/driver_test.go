package mysql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	. "github.com/dogmatiq/projectionkit/sql/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Driver", func() {
	var (
		db      *sql.DB
		closeDB func()
	)

	BeforeEach(func() {
		db, _, closeDB = drivertest.Open(drivertest.MariaDB, "mysql")
	})

	AfterEach(func() {
		if closeDB != nil {
			closeDB()
		}
	})

	drivertest.Declare(
		&Driver{},
		func(ctx context.Context) *sql.DB {
			err := DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return db
		},
	)
})
