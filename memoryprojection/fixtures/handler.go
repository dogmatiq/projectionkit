package fixtures

import (
	"reflect"

	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of [memoryprojection.MessageHandler].
type MessageHandler[T any] struct {
	ConfigureFunc   func(dogma.ProjectionConfigurer)
	NewFunc         func() T
	HandleEventFunc func(T, dogma.ProjectionEventScope, dogma.Message)
	CompactFunc     func(T, dogma.ProjectionCompactScope)
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
//
// Otherwise, if T is a pointer type, it returns a pointer to a new zero-value
// of type *T. Otherwise, it returns a new zero-value of type T.
func (h *MessageHandler[T]) New() T {
	if h.NewFunc != nil {
		return h.NewFunc()
	}

	var zero T

	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		zero = reflect.New(t.Elem()).Interface().(T)
	}

	return zero
}

// HandleEvent handles a domain event message that has been routed to this
// handler.
//
// If h.HandleEventFunc is non-nil, it calls h.HandleEventFunc(v, s, m).
func (h *MessageHandler[T]) HandleEvent(
	v T,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) {
	if h.HandleEventFunc != nil {
		h.HandleEventFunc(v, s, m)
	}
}

// Compact reduces the size of the projection's data.
//
// If h.CompactFunc is non-nil, it calls h.CompactFunc(v, s).
func (h *MessageHandler[T]) Compact(v T, s dogma.ProjectionCompactScope) {
	if h.CompactFunc != nil {
		h.CompactFunc(v, s)
	}
}
