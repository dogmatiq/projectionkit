package boltprojection

import (
	"context"

	"github.com/dogmatiq/dogma"
	"go.etcd.io/bbolt"
)

// MessageHandler is a specialization of dogma.ProjectionMessageHandler that
// persists to a BoltDB database.
type MessageHandler interface {
	// Configure produces a configuration for this handler by calling methods on
	// the configurer c.
	//
	// The implementation MUST allow for multiple calls to Configure(). Each
	// call SHOULD produce the same configuration.
	//
	// The engine MUST call Configure() before calling HandleEvent(). It is
	// RECOMMENDED that the engine only call Configure() once per handler.
	Configure(c dogma.ProjectionConfigurer)

	// HandleEvent updates the projection to reflect the occurrence of an event.
	//
	// Changes to the projection state MUST be performed within the supplied
	// transaction.
	//
	// If nil is returned, the projection state has been persisted successfully.
	//
	// If an error is returned, the projection SHOULD be left in the state it
	// was before HandleEvent() was called.
	//
	// The engine SHOULD provide "at-least-once" delivery guarantees to the
	// handler. That is, the engine should call HandleEvent() with the same
	// event message until a nil error is returned.
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
	HandleEvent(ctx context.Context, tx *bbolt.Tx, s dogma.ProjectionEventScope, m dogma.Event) error

	// Compact reduces the size of the projection's data.
	//
	// The implementation SHOULD attempt to decrease the size of the
	// projection's data by whatever means available. For example, it may delete
	// any unused data, or collapse multiple data sets into one.
	//
	// The context MAY have a deadline. The implementation SHOULD compact data
	// using multiple small transactions, such that if the deadline is reached a
	// future call to Compact() does not need to compact the same data.
	//
	// The engine SHOULD call Compact() repeatedly throughout the lifetime of
	// the projection. The precise scheduling of calls to Compact() are
	// engine-defined. It MAY be called concurrently with any other method.
	Compact(ctx context.Context, db *bbolt.DB, s dogma.ProjectionCompactScope) error
}

// NoCompactBehavior can be embedded in MessageHandler implementations to
// indicate that the projection does not require its data to be compacted.
//
// It provides an implementation of MessageHandler.Compact() that always returns
// a nil error.
type NoCompactBehavior struct{}

// Compact returns nil.
func (NoCompactBehavior) Compact(
	context.Context,
	*bbolt.DB,
	dogma.ProjectionCompactScope,
) error {
	return nil
}
