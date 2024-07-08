package memoryprojection

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/resource"
)

// QueryableMessageHandler is a specialization of the
// [dogma.ProjectionMessageHandler] for in-memory projections that can be
// queried using [Query].
type QueryableMessageHandler[T any] interface {
	dogma.ProjectionMessageHandler
	query(context.Context, func(context.Context, T) error) error
}

// adaptor adapts a [MessageHandler] to the [dogma.ProjectionMessageHandler]
// interface.
type adaptor[T any] struct {
	handler MessageHandler[T]

	m         sync.RWMutex
	resources map[string][]byte
	value     *T
}

// Query accesses the current state of the projection h to produce a result
// value of type R.
//
// fn is called with the current state. The value may be read within the
// lifetime of the call to fn. fn MUST NOT retain a reference to the value after
// the call returns. fn MUST NOT modify the projection state.
func Query[T, R any](
	ctx context.Context,
	h QueryableMessageHandler[T],
	fn func(context.Context, T) (R, error),
) (result R, _ error) {
	return result, h.query(
		ctx,
		func(ctx context.Context, v T) error {
			var err error
			result, err = fn(ctx, v)
			return err
		},
	)
}

// New returns a new Dogma projection message handler that builds an in-memory
// projection using h.
func New[T any](h MessageHandler[T]) QueryableMessageHandler[T] {
	return &adaptor[T]{
		handler:   h,
		resources: map[string][]byte{},
	}
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (a *adaptor[T]) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor[T]) HandleEvent(
	_ context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	a.m.Lock()
	defer a.m.Unlock()

	v := a.resources[string(r)]
	if !bytes.Equal(v, c) {
		return false, nil
	}

	if a.value == nil {
		v := a.handler.New()
		a.value = &v
	}

	a.handler.HandleEvent(*a.value, s, m)
	a.resources[string(r)] = n

	return true, nil
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor[T]) ResourceVersion(_ context.Context, r []byte) ([]byte, error) {
	a.m.RLock()
	defer a.m.RUnlock()

	return a.resources[string(r)], nil
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor[T]) CloseResource(ctx context.Context, r []byte) error {
	return a.DeleteResource(ctx, r)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *adaptor[T]) TimeoutHint(dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data.
func (a *adaptor[T]) Compact(_ context.Context, s dogma.ProjectionCompactScope) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.value != nil {
		a.handler.Compact(*a.value, s)
	}

	return nil
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor[T]) ResourceRepository(context.Context) (resource.Repository, error) {
	return a, nil
}

// StoreResourceVersion sets the version of the resource r to v without
// checking the current version.
func (a *adaptor[T]) StoreResourceVersion(_ context.Context, r, v []byte) error {
	a.m.Lock()
	defer a.m.Unlock()

	a.resources[string(r)] = v

	return nil
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (a *adaptor[T]) UpdateResourceVersion(_ context.Context, r, c, n []byte) (bool, error) {
	a.m.Lock()
	defer a.m.Unlock()

	v := a.resources[string(r)]
	if !bytes.Equal(v, c) {
		return false, nil
	}

	a.resources[string(r)] = n
	return true, nil
}

// DeleteResource removes all information about the resource r.
func (a *adaptor[T]) DeleteResource(_ context.Context, r []byte) error {
	a.m.Lock()
	defer a.m.Unlock()

	delete(a.resources, string(r))
	return nil
}

func (a *adaptor[T]) query(
	ctx context.Context,
	fn func(context.Context, T) error,
) error {
	a.m.RLock()

	// If the value has not been initialized, just pass fn a new empty instance.
	// There's no need to hold the mutex lock in this case as the value is not
	// shared.
	if a.value == nil {
		a.m.RUnlock()
		return fn(ctx, a.handler.New())
	}

	defer a.m.RUnlock()
	return fn(ctx, *a.value)
}
