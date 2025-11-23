package memoryprojection

import (
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a specialization of [dogma.ProjectionMessageHandler] that
// builds an in-memory projection represented by a value of type T.
type MessageHandler[T any] interface {
	// Configure declares the handler's configuration by calling methods on c.
	//
	// The configuration includes the handler's identity and message routes.
	//
	// The engine calls this method at least once during startup. It must
	// produce the same configuration each time it's called.
	Configure(c dogma.ProjectionConfigurer)

	// HandleEvent updates the projection to reflect the occurrence of a
	// [dogma.Event]. It may do so by modifying v in-place then returning it, or
	// by returning an entirely new value.
	HandleEvent(v T, s dogma.ProjectionEventScope, m dogma.Event) (T, error)

	// Compact reduces the projection's size by removing or consolidating data.
	// It may do so by modifying v in-place then returning it, or by returning
	// an entirely new value.
	Compact(v T, s dogma.ProjectionCompactScope) T
}

// NoCompactBehavior can be embedded in MessageHandler implementations to
// indicate that the projection does not require its data to be compacted.
//
// It provides an implementation of MessageHandler.Compact() that does nothing.
type NoCompactBehavior[T any] struct{}

// Compact does nothing.
func (NoCompactBehavior[T]) Compact(v T, _ dogma.ProjectionCompactScope) T {
	return v
}
