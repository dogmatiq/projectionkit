package memoryprojection

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/resource"
)

// Projection is an in-memory projection that builds a value of type T.
type Projection[T any, H MessageHandler[T]] struct {
	handler H

	m         sync.RWMutex
	resources map[string][]byte
	value     T
}

// Query queries a value of type T to produce a result of type R.
//
// q is called with the current value, which may be read within the lifetime of
// the call to fn. fn MUST NOT retain a reference to the value after the call
// returns. fn MUST NOT modify the value.
func Query[T, R any, H MessageHandler[T]](
	p *Projection[T, H],
	q func(T) R,
) R {
	p.m.RLock()
	defer p.m.RUnlock()

	return q(p.value)
}

// New returns a new projection that uses the given handler to build an
// in-memory value of type T.
func New[H MessageHandler[T], T any](h H) *Projection[T, H] {
	return &Projection[T, H]{
		handler: h,
	}
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (p *Projection[T, H]) Configure(c dogma.ProjectionConfigurer) {
	p.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (p *Projection[T, H]) HandleEvent(
	_ context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	p.m.Lock()
	defer p.m.Unlock()

	v := p.resources[string(r)]
	if !bytes.Equal(v, c) {
		return false, nil
	}

	value, err := p.handler.HandleEvent(p.value, s, m)
	if err != nil {
		return false, err
	}

	if p.resources == nil {
		p.resources = make(map[string][]byte)
	}
	p.resources[string(r)] = n
	p.value = value

	return true, nil
}

// ResourceVersion returns the version of the resource r.
func (p *Projection[T, H]) ResourceVersion(_ context.Context, r []byte) ([]byte, error) {
	p.m.RLock()
	defer p.m.RUnlock()

	return p.resources[string(r)], nil
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (p *Projection[T, H]) CloseResource(ctx context.Context, r []byte) error {
	return p.DeleteResource(ctx, r)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (p *Projection[T, H]) TimeoutHint(dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data.
func (p *Projection[T, H]) Compact(_ context.Context, s dogma.ProjectionCompactScope) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.resources != nil {
		// Only attempt to compact the value if some events have been applied.
		p.value = p.handler.Compact(p.value, s)
	}

	return nil
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (p *Projection[T, H]) ResourceRepository(context.Context) (resource.Repository, error) {
	return p, nil
}

// StoreResourceVersion sets the version of the resource r to v without
// checking the current version.
func (p *Projection[T, H]) StoreResourceVersion(_ context.Context, r, v []byte) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.resources == nil {
		p.resources = make(map[string][]byte)
	}
	p.resources[string(r)] = v

	return nil
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (p *Projection[T, H]) UpdateResourceVersion(_ context.Context, r, c, n []byte) (bool, error) {
	p.m.Lock()
	defer p.m.Unlock()

	v := p.resources[string(r)]
	if !bytes.Equal(v, c) {
		return false, nil
	}

	if p.resources == nil {
		p.resources = make(map[string][]byte)
	}
	p.resources[string(r)] = n
	return true, nil
}

// DeleteResource removes all information about the resource r.
func (p *Projection[T, H]) DeleteResource(_ context.Context, r []byte) error {
	p.m.Lock()
	defer p.m.Unlock()

	delete(p.resources, string(r))
	return nil
}
