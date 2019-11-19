package boltdb_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/fixtures"
	. "github.com/dogmatiq/projectionkit/boltdb"
	"github.com/dogmatiq/projectionkit/boltdb/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type adaptor", func() {
	var (
		handler *messageHandlerMock
		db      *internal.TempDB
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		handler = &messageHandlerMock{}
		handler.ConfigureCall = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}
		db = internal.NewTempDB()
		adaptor = New(db.DB, handler)
	})

	AfterEach(func() {
		db.Close()
	})

	Context("func HandleEvent()", func() {
		It("executes as expected", func() {

			By("persisting the initial resource version")

			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
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
				fixtures.MessageA2,
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
				fixtures.MessageA3,
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

		It("returns an error if MessageHandler's HandleEvent method fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventCall = func(
				context.Context,
				*bbolt.Tx,
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
				fixtures.MessageA1,
			)

			Expect(ok).Should(BeFalse())
			Expect(err).Should(HaveOccurred())
		})

		It("returns false if the currect resource version in the database is incorrect", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			ok, err = adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				[]byte("<incorrect current version>"),
				[]byte("<version 02>"),
				nil,
				fixtures.MessageA2,
			)

			Expect(ok).Should(BeFalse())
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("func ResourceVersion()", func() {
		It("returns a resource version", func() {

			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA1,
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
	})

	Context("func CloseResource()", func() {
		It("removes a resource version", func() {
			ok, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				fixtures.MessageA2,
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
})
