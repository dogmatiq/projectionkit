package kvprojection

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dogmatiq/dogma"
)

func New(
	h MessageHandler,
	s KeyValueStore,
) dogma.ProjectionMessageHandler {
	return &adaptor{
		handler: h,
		store:   s,
	}
}

type adaptor struct {
	dogma.NoTimeoutHintBehavior

	handler    MessageHandler
	handlerKey []byte
	store      KeyValueStore
}

var _ dogma.ProjectionMessageHandler = (*adaptor)(nil)

func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

func (a *adaptor) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	e dogma.Event,
) (bool, error) {
	changeset := []Change{
		{
			resourceKey(a.handlerKey, r),
			c,
			n,
		},
	}

	for _, key := range a.handler.RouteEventToKeys(e) {
		if isResourceKey(key) {
			panic(fmt.Sprintf(
				"keys beginning with %q are reserved",
				string(resourceKeyPrefix),
			))
		}

		before, err := a.store.Load(ctx, key)
		if err != nil {
			return false, err
		}

		after := a.handler.HandleEvent(key, before, s, e)

		if !bytes.Equal(before, after) {
			changeset = append(changeset, Change{key, before, after})
		}
	}

	return a.store.Save(ctx, changeset...)

}

func (a *adaptor) Compact(
	ctx context.Context,
	s dogma.ProjectionCompactScope,
) error {
	return a.store.Range(
		ctx,
		func(ctx context.Context, key, before []byte) error {
			if isResourceKey(key) {
				return nil
			}

			after := a.handler.Compact(key, before, s)

			if bytes.Equal(before, after) {
				return nil
			}

			_, err := a.store.Save(ctx, Change{key, before, after})
			return err
		},
	)
}

func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return a.store.Load(ctx, resourceKey(a.handlerKey, r))
}

func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	key := resourceKey(a.handlerKey, r)

	for {
		c, err := a.store.Load(ctx, key)
		if err != nil {
			return err
		}

		ok, err := a.store.Save(ctx, Change{key, c, nil})
		if ok || err != nil {
			return err
		}
	}
}
