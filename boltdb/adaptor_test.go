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

var _ = Describe("MessageHandler Adaptor", func() {
	var (
		mh *internal.MessageHandlerMock
		db *internal.TempDB
	)

	BeforeEach(func() {
		mh = &internal.MessageHandlerMock{}
		mh.ConfigureCall = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}
		db = internal.NewTempDB()
	})

	AfterEach(func() {
		db.Close()
	})

	Context("HandleEvent method", func() {

		It("executes as expected", func() {
			ctx := context.TODO()
			a := New(db.DB, mh)
			r := []byte("<resource>")
			c := []byte{}
			n := []byte("<version 01>")

			By("peristing the correct initial version")

			ok, err := a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA1,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err := a.ResourceVersion(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal(n))

			By("peristing the next correct version")

			c = n
			n = []byte("<version 02>")

			ok, err = a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA2,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err = a.ResourceVersion(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal(n))

			By("deleting resource if the next version is empty")

			c = n
			n = []byte{}

			ok, err = a.HandleEvent(
				ctx,
				r,
				c,
				nil,
				nil,
				fixtures.MessageA3,
			)
			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err = a.ResourceVersion(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeNil())
		})

		It("returns an error if MessageHandler's HandleEvent method fails", func() {
			ctx := context.TODO()
			a := New(db.DB, mh)
			r := []byte("<resource>")
			c := []byte{}
			n := []byte("<version 01>")
			terr := errors.New("handle event test error")

			mh.HandleEventCall = func(
				context.Context,
				*bbolt.Tx,
				dogma.ProjectionEventScope,
				dogma.Message,
			) error {
				return terr
			}

			ok, err := a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA1,
			)

			Expect(ok).Should(BeFalse())
			Expect(err).Should(HaveOccurred())
		})

		It("returns false if the currect version in the database is incorrect", func() {
			ctx := context.TODO()
			a := New(db.DB, mh)
			r := []byte("<resource>")
			c := []byte{}
			n := []byte("<version 01>")

			ok, err := a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA1,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			c = []byte("<incorrect current version>")
			n = []byte("<version 02>")

			ok, err = a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA2,
			)

			Expect(ok).Should(BeFalse())
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("ResourceVersion method", func() {
		It("returns the correct resource version", func() {
			ctx := context.TODO()
			a := New(db.DB, mh)
			r := []byte("<resource>")
			c := []byte{}
			n := []byte("<version 01>")

			ok, err := a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA2,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			v, err := a.ResourceVersion(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(Equal(n))
		})

		It("CloseResource method", func() {
			ctx := context.TODO()
			a := New(db.DB, mh)
			r := []byte("<resource>")
			c := []byte{}
			n := []byte("<version 01>")

			ok, err := a.HandleEvent(
				ctx,
				r,
				c,
				n,
				nil,
				fixtures.MessageA2,
			)

			Expect(ok).Should(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())

			err = a.CloseResource(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())

			v, err := a.ResourceVersion(ctx, r)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeNil())
		})
	})
})
