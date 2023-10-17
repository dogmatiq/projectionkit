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
	It("uses the repository if the handler implements RepositoryAware", func() {
		err := StoreVersion(
			context.Background(),
			&repositoryAwareStub{},
			[]byte("<resource>"),
			[]byte("<next>"),
		)

		Expect(err).To(MatchError("<store error>"))
	})

	It("returns an error if the handler does not implement RepositoryAware", func() {
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
	It("uses the repository if the handler implements RepositoryAware", func() {
		_, err := UpdateVersion(
			context.Background(),
			&repositoryAwareStub{},
			[]byte("<resource>"),
			[]byte("<current>"),
			[]byte("<next>"),
		)

		Expect(err).To(MatchError("<update error>"))
	})

	It("returns an error if the handler does not implement RepositoryAware", func() {
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
	It("uses the repository if the handler implements RepositoryAware", func() {
		err := DeleteResource(
			context.Background(),
			&repositoryAwareStub{},
			[]byte("<resource>"),
		)

		Expect(err).To(MatchError("<delete error>"))
	})

	It("returns an error if the handler does not implement RepositoryAware", func() {
		err := DeleteResource(
			context.Background(),
			&ProjectionMessageHandler{},
			[]byte("<resource>"),
		)

		Expect(err).To(Equal(ErrNotSupported))
	})
})

type repositoryAwareStub struct {
	ProjectionMessageHandler
}

func (*repositoryAwareStub) ResourceRepository(context.Context) (Repository, error) {
	return repositoryStub{}, nil
}

type repositoryStub struct {
}

func (repositoryStub) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return []byte("<version>"), nil
}

func (repositoryStub) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	return errors.New("<store error>")
}

func (repositoryStub) UpdateResourceVersion(ctx context.Context, r, c, n []byte) (bool, error) {
	return false, errors.New("<update error>")
}

func (repositoryStub) DeleteResource(ctx context.Context, r []byte) error {
	return errors.New("<delete error>")
}
