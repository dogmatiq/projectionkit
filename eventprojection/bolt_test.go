package eventprojection_test

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/projectionkit/eventprojection"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type boltAdaptor", func() {
	var (
		ctx      context.Context
		cancel   context.CancelFunc
		handler  *messageHandlerStub
		db       *bbolt.DB
		tmpfile  string
		adaptor  dogma.ProjectionMessageHandler
		consumer Consumer
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)

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

		handler.MarshalEventFunc = func(ev Event) ([]byte, error) {
			return []byte(ev.(string)), nil
		}

		handler.UnmarshalEventFunc = func(b []byte) (Event, error) {
			return string(b), nil
		}

		adaptor, consumer = NewBolt(db, handler)
	})

	AfterEach(func() {
		if cancel != nil {
			cancel()
		}

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
		It("forwards to the handler", func() {
			called := false
			handler.HandleEventFunc = func(
				es EventScope,
				m dogma.Message,
			) {
				called = true
				Expect(m).To(Equal(MessageA1))
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("stores the recorded events", func() {
			handler.HandleEventFunc = func(
				es EventScope,
				m dogma.Message,
			) {
				es.RecordEvent("<stream>", "<event 1>")
				es.RecordEvent("<stream>", "<event 2>")
				es.RecordEvent("<stream>", "<event 3>")
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(err).ShouldNot(HaveOccurred())

			cur, err := consumer.OpenAt(ctx, "<stream>", 0)
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			ev, ok, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(ev).To(Equal("<event 1>"))

			ev, ok, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(ev).To(Equal("<event 2>"))

			ev, ok, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(ev).To(Equal("<event 3>"))
		})
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
