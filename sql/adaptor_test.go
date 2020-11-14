// +build cgo

package sql_test

import (
	"context"
	"database/sql"
	"errors"

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
		closeDB func()
		handler *fixtures.MessageHandler
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		db, _, closeDB = drivertest.Open("sqlite3")
	})

	BeforeEach(func() {
		handler = &fixtures.MessageHandler{}
		adaptor = MustNew(db, handler, nil)
	})

	AfterEach(func() {
		if closeDB != nil {
			closeDB()
		}
	})

	adaptortest.Declare(
		func(ctx context.Context) dogma.ProjectionMessageHandler {
			err := sqlite.DropSchema(context.Background(), db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(context.Background(), db)
			Expect(err).ShouldNot(HaveOccurred())

			return adaptor
		},
	)

	Describe("func Closure()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d *sql.DB,
				_ dogma.ProjectionCompactScope,
			) error {
				Expect(d).To(BeIdenticalTo(db))
				return errors.New("<error>")
			}

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("<error>"))
		})
	})
})

var _ = Describe("func MustNew()", func() {
	It("panics on failure", func() {
		Expect(func() {
			MustNew(
				drivertest.MockDB(),
				&fixtures.MessageHandler{},
				nil,
			)
		}).To(PanicWith(
			MatchError("can not deduce the appropriate SQL projection driver for *drivertest.MockDriver"),
		))
	})
})
