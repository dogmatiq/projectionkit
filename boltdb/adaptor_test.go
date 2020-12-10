package boltdb_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/projectionkit/boltdb"
	"github.com/dogmatiq/projectionkit/boltdb/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

var _ = Describe("type adaptor", func() {
	var (
		handler *fixtures.MessageHandler
		db      *bolt.DB
		tmpfile string
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		f, err := ioutil.TempFile("", "*.boltdb")
		Expect(err).ShouldNot(HaveOccurred())
		f.Close()

		tmpfile = f.Name()

		db, err = bolt.Open(tmpfile, 0600, bolt.DefaultOptions)
		Expect(err).ShouldNot(HaveOccurred())

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
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

	adaptortest.Declare(
		func(ctx context.Context) dogma.ProjectionMessageHandler {
			return adaptor
		},
	)

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

	Describe("func HandleEvent()", func() {
		It("does not produce errors when OCC parameters are supplied correctly", func() {
			By("persisting the initial resource version")

			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal([]byte("<version 01>")))

			By("persisting the next resource version")

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<version 01>"),
				[]byte("<version 02>"),
				nil,
				MessageA2,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err = adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal([]byte("<version 02>")))

			By("discarding a resource if the next resource version is empty")

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<version 02>"),
				nil,
				nil,
				MessageA3,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err = adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeEmpty())
		})

		It("returns an error if the application's message handler fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventFunc = func(
				context.Context,
				*bolt.Tx,
				dogma.ProjectionEventScope,
				dogma.Message,
			) error {
				return terr
			}

			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(ok).Should(BeFalse())
			Expect(err).Should(HaveOccurred())
		})

		It("returns false if supplied resource version is not the current version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<incorrect current version>"),
				[]byte("<version 02>"),
				nil,
				MessageA2,
			)
			Expect(ok).Should(BeFalse())
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func ResourceVersion()", func() {
		It("returns a resource version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal([]byte("<version 01>")))
		})

		It("returns nil if no current resource version present in the database", func() {
			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeEmpty())
		})
	})

	Describe("func CloseResource()", func() {
		It("removes a resource version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA2,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			err = adaptor.CloseResource(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())

			v, err := adaptor.ResourceVersion(
				context.Background(),
				[]byte("<resource>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeEmpty())
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
