package resource_test

import (
	"context"

	"github.com/dogmatiq/dogma"
)

type storerDecorator struct {
	dogma.ProjectionMessageHandler

	StoreResourceVersionFunc func(ctx context.Context, r, v []byte) error
}

func (d storerDecorator) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	return d.StoreResourceVersionFunc(ctx, r, v)
}

type updaterDecorator struct {
	dogma.ProjectionMessageHandler

	UpdateResourceVersionFunc func(ctx context.Context, r, c, n []byte) (bool, error)
}

func (d updaterDecorator) UpdateResourceVersion(ctx context.Context, r, c, n []byte) (bool, error) {
	return d.UpdateResourceVersionFunc(ctx, r, c, n)
}

type deleterDecorator struct {
	dogma.ProjectionMessageHandler

	DeleteResourceFunc func(ctx context.Context, r []byte) error
}

func (d deleterDecorator) DeleteResource(ctx context.Context, r []byte) error {
	return d.DeleteResourceFunc(ctx, r)
}
