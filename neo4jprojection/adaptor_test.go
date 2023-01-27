package neo4jprojection_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/neo4jprojection"
	"github.com/dogmatiq/projectionkit/neo4jprojection/fixtures" // can't dot-import due to conflict
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var (
		ctx     context.Context
		handler *fixtures.MessageHandler
		db      neo4j.DriverWithContext
		tmpfile string
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		f, err := ioutil.TempFile("", "*.boltdb")
		Expect(err).ShouldNot(HaveOccurred())
		f.Close()

		tmpfile = f.Name()

		db, err = neo4j.NewDriverWithContext("target string", neo4j.BasicAuth("username", "password", ""))
		//db, err = bbolt.Open(tmpfile, 0600, bbolt.DefaultOptions)
		Expect(err).ShouldNot(HaveOccurred())

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		adaptor = New(db, handler)
	})

	AfterEach(func() {
		if db != nil {
			db.Close(ctx)
		}

		if tmpfile != "" {
			os.Remove(tmpfile)
		}
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func New()", func() {
		It("returns an unbound handler if the database is nil", func() {
			adaptor = New(nil, handler)

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(adaptor)).To(Equal("<key>"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("returns an error if the application's message handler fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventFunc = func(
				context.Context,
				neo4j.ExplicitTransaction,
				dogma.ProjectionEventScope,
				dogma.Message,
			) error {
				return terr
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func TimeoutHint()", func() {
		It("forwards to the handler", func() {
			handler.TimeoutHintFunc = func(
				m dogma.Message,
			) time.Duration {
				Expect(m).To(BeIdenticalTo(MessageA1))
				return 100 * time.Millisecond
			}

			d := adaptor.TimeoutHint(
				MessageA1,
			)
			Expect(d).To(Equal(100 * time.Millisecond))
		})
	})

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d neo4j.DriverWithContext,
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
