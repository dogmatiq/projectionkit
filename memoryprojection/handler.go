package memoryprojection

import (
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a specialization of [dogma.ProjectionMessageHandler] that
// builds an in-memory projection represented by a value of type T.
type MessageHandler[T any] interface {
	// Configure produces a configuration for this handler by calling methods on
	// the configurer c.
	//
	// The implementation MUST allow for multiple calls to Configure(). Each
	// call SHOULD produce the same configuration.
	//
	// The engine MUST call Configure() before calling HandleEvent(). It is
	// RECOMMENDED that the engine only call Configure() once per handler.
	Configure(c dogma.ProjectionConfigurer)

	// New returns a new instance of the projection's data type.
	New() T

	// HandleEvent updates the projection to reflect the occurrence of an event.
	// It may do so by modifying v in-place then returning it, or by returning
	// an entirely new value.
	//
	// The engine MAY provide guarantees about the order in which event messages
	// will be passed to HandleEvent(), however in the interest of engine
	// portability the implementation SHOULD NOT assume that HandleEvent() will
	// be called with events in the same order that they were recorded.
	//
	// The engine MUST NOT call HandleEvent() with any message of a type that
	// has not been configured for consumption by a prior call to Configure().
	// If any such message is passed, the implementation MUST panic with the
	// UnexpectedMessage value.
	//
	// The engine MAY call HandleEvent() from multiple goroutines concurrently.
	HandleEvent(v T, s dogma.ProjectionEventScope, m dogma.Message) (T, error)

	// Compact reduces the size of the projection's data. It may do so by
	// modifying v in-place then returning it, or by returning an entirely new
	// value.
	//
	// The implementation SHOULD attempt to decrease the size of the
	// projection's data by whatever means available. For example, it may delete
	// any unused data, or collapse multiple data sets into one.
	//
	// The engine SHOULD call Compact() repeatedly throughout the lifetime of
	// the projection. The precise scheduling of calls to Compact() are
	// engine-defined. It MAY be called concurrently with any other method.
	Compact(v T, s dogma.ProjectionCompactScope) T
}

// NoCompactBehavior can be embedded in MessageHandler implementations to
// indicate that the projection does not require its data to be compacted.
//
// It provides an implementation of MessageHandler.Compact() that does nothing.
type NoCompactBehavior[T any] struct{}

// Compact does nothing.
func (NoCompactBehavior[T]) Compact(T, dogma.ProjectionCompactScope) {}
