package resource_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/projectionkit/resource"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func StoreVersion()", func() {
	Context("when the handler implements the storer interface", func() {
		It("uses the storer interface", func() {
			d := &storerDecorator{
				StoreResourceVersionFunc: func(
					_ context.Context,
					r, v []byte,
				) error {
					Expect(r).To(Equal([]byte("<resource>")))
					Expect(v).To(Equal([]byte("<version>")))
					return errors.New("<error>")
				},
			}

			err := StoreVersion(
				context.Background(),
				d,
				[]byte("<resource>"),
				[]byte("<version>"),
			)

			Expect(err).To(MatchError("<error>"))
		})
	})

	Context("when the handler implements the updater interface", func() {
		It("propagates errors when reading the current version", func() {
			d := &updaterDecorator{
				ProjectionMessageHandler: &ProjectionMessageHandler{
					ResourceVersionFunc: func(ctx context.Context, r []byte) ([]byte, error) {
						return nil, errors.New("<error>")
					},
				},
			}

			err := StoreVersion(
				context.Background(),
				d,
				[]byte("<resource>"),
				[]byte("<next>"),
			)

			Expect(err).To(MatchError("<error>"))
		})

		It("first reads the version, then performs an update", func() {
			d := &updaterDecorator{
				ProjectionMessageHandler: &ProjectionMessageHandler{
					ResourceVersionFunc: func(ctx context.Context, r []byte) ([]byte, error) {
						return []byte("<current>"), nil
					},
				},
				UpdateResourceVersionFunc: func(
					_ context.Context,
					r, c, n []byte,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<resource>")))
					Expect(c).To(Equal([]byte("<current>")))
					Expect(n).To(Equal([]byte("<next>")))
					return false, errors.New("<error>")
				},
			}

			err := StoreVersion(
				context.Background(),
				d,
				[]byte("<resource>"),
				[]byte("<next>"),
			)

			Expect(err).To(MatchError("<error>"))
		})

		It("retries after an OCC failure", func() {
			var isRetry bool
			d := &updaterDecorator{
				ProjectionMessageHandler: &ProjectionMessageHandler{
					ResourceVersionFunc: func(ctx context.Context, r []byte) ([]byte, error) {
						if isRetry {
							return []byte("<second>"), nil
						}

						return []byte("<first>"), nil
					},
				},
				UpdateResourceVersionFunc: func(
					_ context.Context,
					r, c, n []byte,
				) (bool, error) {
					if isRetry {
						Expect(r).To(Equal([]byte("<resource>")))
						Expect(c).To(Equal([]byte("<second>")))
						Expect(n).To(Equal([]byte("<next>")))
						return true, nil
					}

					isRetry = true
					Expect(r).To(Equal([]byte("<resource>")))
					Expect(c).To(Equal([]byte("<first>")))
					Expect(n).To(Equal([]byte("<next>")))
					return false, nil
				},
			}

			err := StoreVersion(
				context.Background(),
				d,
				[]byte("<resource>"),
				[]byte("<next>"),
			)

			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	It("returns an error if the handler implements neither storer nor updater", func() {
		err := StoreVersion(
			context.Background(),
			&ProjectionMessageHandler{},
			[]byte("<resource>"),
			[]byte("<version>"),
		)

		Expect(err).To(Equal(ErrNotSupported))
	})
})

var _ = Describe("func UpdateVersion()", func() {
	Context("when the handler implements the updater interface", func() {
		It("uses the updater interface", func() {
			d := &updaterDecorator{
				UpdateResourceVersionFunc: func(
					_ context.Context,
					r, c, n []byte,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<resource>")))
					Expect(c).To(Equal([]byte("<current>")))
					Expect(n).To(Equal([]byte("<next>")))
					return true, errors.New("<error>")
				},
			}

			ok, err := UpdateVersion(
				context.Background(),
				d,
				[]byte("<resource>"),
				[]byte("<current>"),
				[]byte("<next>"),
			)

			Expect(ok).To(BeTrue())
			Expect(err).To(MatchError("<error>"))
		})
	})

	It("returns an error if the handler does not implement updater", func() {
		_, err := UpdateVersion(
			context.Background(),
			&ProjectionMessageHandler{},
			[]byte("<resource>"),
			[]byte("<current>"),
			[]byte("<next>"),
		)

		Expect(err).To(Equal(ErrNotSupported))
	})
})

var _ = Describe("func DeleteResource()", func() {
	Context("when the handler implements the deleter interface", func() {
		It("uses the deleter interface", func() {
			d := &deleterDecorator{
				DeleteResourceFunc: func(
					_ context.Context,
					r []byte,
				) error {
					Expect(r).To(Equal([]byte("<resource>")))
					return errors.New("<error>")
				},
			}

			err := DeleteResource(
				context.Background(),
				d,
				[]byte("<resource>"),
			)

			Expect(err).To(MatchError("<error>"))
		})
	})

	It("returns an error if the handler does not implement deleter", func() {
		err := DeleteResource(
			context.Background(),
			&ProjectionMessageHandler{},
			[]byte("<resource>"),
		)

		Expect(err).To(Equal(ErrNotSupported))
	})
})
