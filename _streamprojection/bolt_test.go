package streamprojection_test

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/streamprojection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type adaptor (with BoltDB repository)", func() {
	var (
		ctx     context.Context
		handler *messageHandlerStub
		db      *bbolt.DB
		tmpfile string
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		f, err := ioutil.TempFile("", "*.boltdb")
		Expect(err).ShouldNot(HaveOccurred())
		f.Close()

		tmpfile = f.Name()

		db, err = bbolt.Open(tmpfile, 0600, bbolt.DefaultOptions)
		Expect(err).ShouldNot(HaveOccurred())

		handler = &messageHandlerStub{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		adaptor, _ = NewBolt(db, handler)
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

	Describe("func NewBolt()", func() {
		It("returns an unbound handler if the database is nil", func() {
			adaptor, _ = NewBolt(nil, handler)

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
		BeforeEach(func() {
			handler.RouteEventToStreamsFunc = func(m dogma.Message) []string {
				return []string{"<stream>"}
			}
		})

		It("forwards to the handler", func() {
			handler.HandleEventFunc = func(
				s Snapshot,
				es EventScope,
				m dogma.Message,
			) {

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

		// It("returns an error if the application's message handler fails", func() {
		// 	terr := errors.New("handle event test error")

		// 	handler.HandleEventFunc = func(
		// 		context.Context,
		// 		*bbolt.Tx,
		// 		dogma.ProjectionEventScope,
		// 		dogma.Message,
		// 	) error {
		// 		return terr
		// 	}

		// 	_, err := adaptor.HandleEvent(
		// 		context.Background(),
		// 		[]byte("<resource>"),
		// 		nil,
		// 		[]byte("<version 01>"),
		// 		nil,
		// 		MessageA1,
		// 	)
		// 	Expect(err).Should(HaveOccurred())
		// })
	})

	Describe("func TimeoutHint()", func() {
		It("returns zero", func() {
			d := adaptor.TimeoutHint(MessageA1)
			Expect(d).To(Equal(0 * time.Millisecond))
		})
	})

	Describe("func Compact()", func() {
		// It("forwards to the handler", func() {
		// 	handler.CompactFunc = func(
		// 		_ context.Context,
		// 		d *bbolt.DB,
		// 		_ dogma.ProjectionCompactScope,
		// 	) error {
		// 		Expect(d).To(BeIdenticalTo(db))
		// 		return errors.New("<error>")
		// 	}

		// 	err := adaptor.Compact(
		// 		context.Background(),
		// 		nil, // scope
		// 	)
		// 	Expect(err).To(MatchError("<error>"))
		// })
	})
})
