package boltprojection_test

import (
	"context"
	"errors"
	"os"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/boltprojection/internal/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type adaptor", func() {
	var (
		ctx     context.Context
		handler *fixtures.MessageHandler
		db      *bbolt.DB
		tmpfile string
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		f, err := os.CreateTemp("", "*.boltdb")
		Expect(err).ShouldNot(HaveOccurred())
		f.Close()

		tmpfile = f.Name()

		db, err = bbolt.Open(tmpfile, 0600, bbolt.DefaultOptions)
		Expect(err).ShouldNot(HaveOccurred())

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "87bb65eb-a5db-4213-8f5c-4ddbc97aa711")
		}

		adaptor = New(db, handler)
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}

		if tmpfile != "" {
			os.Remove(tmpfile)
		}
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(adaptor).AsString()).To(Equal("87bb65eb-a5db-4213-8f5c-4ddbc97aa711"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("returns an error if the application's message handler fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventFunc = func(
				context.Context,
				*bbolt.Tx,
				dogma.ProjectionEventScope,
				dogma.Event,
			) error {
				return terr
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				&stubs.ProjectionEventScopeStub{},
				EventA1,
			)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d *bbolt.DB,
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
