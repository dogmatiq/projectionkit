package memoryprojection

import (
	"context"
	"sync"

	"github.com/dogmatiq/dogma"
)

// Projection is an in-memory projection that builds a value of type T.
type Projection[T any, H MessageHandler[T]] struct {
	Handler H

	m           sync.RWMutex
	checkpoints map[string]uint64
	value       T
}

// Query queries a value of type T to produce a result of type R.
//
// q is called with the current value, which may be read within the lifetime of
// the call to fn. fn MUST NOT retain a reference to the value after the call
// returns. fn MUST NOT modify the value.
func Query[T, R any, H MessageHandler[T]](p *Projection[T, H], q func(T) R) R {
	p.m.RLock()
	defer p.m.RUnlock()

	return q(p.value)
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (p *Projection[T, H]) Configure(c dogma.ProjectionConfigurer) {
	p.Handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (p *Projection[T, H]) HandleEvent(
	_ context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) (uint64, error) {
	p.m.Lock()
	defer p.m.Unlock()

	id := s.StreamID()
	cp := p.checkpoints[id]

	if s.CheckpointOffset() != cp {
		return cp, nil
	}

	value, err := p.Handler.HandleEvent(p.value, s, m)
	if err != nil {
		return 0, err
	}

	if p.checkpoints == nil {
		p.checkpoints = map[string]uint64{}
	}

	cp = s.Offset() + 1
	p.checkpoints[id] = cp
	p.value = value

	return cp, nil
}

// CheckpointOffset returns the offset at which the handler expects to
// resume handling events from a specific stream.
func (p *Projection[T, H]) CheckpointOffset(_ context.Context, id string) (uint64, error) {
	p.m.RLock()
	defer p.m.RUnlock()

	return p.checkpoints[id], nil

}

// Compact reduces the size of the projection's data.
func (p *Projection[T, H]) Compact(_ context.Context, s dogma.ProjectionCompactScope) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.checkpoints != nil {
		// Only attempt to compact the value if some events have been applied.
		p.value = p.Handler.Compact(p.value, s)
	}

	return nil
}

// Reset resets the projection to its initial state.
func (p *Projection[T, H]) Reset(context.Context, dogma.ProjectionResetScope) error {
	p.m.Lock()
	defer p.m.Unlock()

	p.checkpoints = nil
	var zero T
	p.value = zero

	return nil
}
