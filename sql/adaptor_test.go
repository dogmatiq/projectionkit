// +build cgo

package sql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	. "github.com/dogmatiq/projectionkit/sql"
	"github.com/dogmatiq/projectionkit/sql/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/sql/internal/drivertest"
	"github.com/dogmatiq/projectionkit/sql/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var (
		db      *sql.DB
		handler *fixtures.MessageHandler
	)

	BeforeSuite(func() {
		db = drivertest.Open("sqlite3")
	})

	AfterSuite(func() {
		if db != nil {
			db.Close()
		}
	})

	adaptortest.Declare(
		func(ctx context.Context) dogma.ProjectionMessageHandler {
			err := sqlite.DropSchema(context.Background(), db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(context.Background(), db)
			Expect(err).ShouldNot(HaveOccurred())

			handler = &fixtures.MessageHandler{}

			adaptor, err := New(db, handler, nil)
			Expect(err).ShouldNot(HaveOccurred())

			return adaptor
		},
	)
})
