package fixtures

import (
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of [memoryprojection.MessageHandler].
type MessageHandler[T any] struct {
	ConfigureFunc   func(dogma.ProjectionConfigurer)
	NewFunc         func() T
	HandleEventFunc func(T, dogma.ProjectionEventScope, dogma.Message) (T, error)
	CompactFunc     func(T, dogma.ProjectionCompactScope) T
}

// Configure configures the behavior of the engine as it relates to this
// handler.
//
// c provides access to the various configuration options, such as specifying
// which types of event messages are routed to this handler.
//
// If h.ConfigureFunc is non-nil, it calls h.ConfigureFunc(c).
func (h *MessageHandler[T]) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

// New returns a new instance of the projection's data.
//
// If h.NewFunc is non-nil, it calls h.NewFunc().
// Otherwise, it returns a zero-value.
func (h *MessageHandler[T]) New() T {
	if h.NewFunc != nil {
		return h.NewFunc()
	}

	var zero T
	return zero
}

// HandleEvent handles a domain event message that has been routed to this
// handler.
//
// If h.HandleEventFunc is non-nil, it returns h.HandleEventFunc(v, s, m).
// Otherwise, it returns v unmodified.
func (h *MessageHandler[T]) HandleEvent(
	v T,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (T, error) {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(v, s, m)
	}
	return v, nil
}

// Compact reduces the size of the projection's data.
//
// If h.CompactFunc is non-nil, it returns h.CompactFunc(v, s). Otherwise, it
// returns v unmodified.
func (h *MessageHandler[T]) Compact(v T, s dogma.ProjectionCompactScope) T {
	if h.CompactFunc != nil {
		return h.CompactFunc(v, s)
	}
	return v
}
