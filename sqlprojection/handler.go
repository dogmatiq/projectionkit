package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
)

// MessageHandler is a specialization of dogma.ProjectionMessageHandler that
// persists to an SQL database.
type MessageHandler interface {
	// Configure declares the handler's configuration by calling methods on c.
	//
	// The configuration includes the handler's identity and message routes.
	//
	// The engine calls this method at least once during startup. It must
	// produce the same configuration each time it's called.
	Configure(c dogma.ProjectionConfigurer)

	// HandleEvent updates the projection to reflect the occurrence of a
	// [dogma.Event].
	//
	// Changes to the projection's data must be performed within the supplied
	// transaction.
	HandleEvent(ctx context.Context, tx *sql.Tx, s dogma.ProjectionEventScope, m dogma.Event) error

	// Compact reduces the projection's size by removing or consolidating data.
	//
	// The handler might delete obsolete entries or merge fine-grained data into
	// summaries. The specific strategy depends on the projection's purpose and
	// access patterns.
	//
	// The implementation should perform compaction incrementally to make some
	// progress even if ctx reaches its deadline.
	//
	// The engine may call this method at any time, including in parallel with
	// handling an event.
	//
	// Not all projections need compaction. Embed [NoCompactBehavior] in the
	// handler to indicate compaction not required.
	Compact(ctx context.Context, db *sql.DB, s dogma.ProjectionCompactScope) error

	// Reset clears all projection data.
	//
	// Changes to the projection's data must be performed within the supplied
	// transaction.
	//
	// Not all projections can be reset. Embed [NoResetBehavior] in the handler
	// to indicate that reset is not supported.
	Reset(ctx context.Context, tx *sql.Tx, s dogma.ProjectionResetScope) error
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
	*sql.DB,
	dogma.ProjectionCompactScope,
) error {
	return nil
}

// NoResetBehavior is an embeddable type for [MessageHandler] implementations
// that don't support resetting their state.
//
// Embed this type in a [MessageHandler] when resetting projection data isn't
// feasible or required.
type NoResetBehavior struct{}

// Reset returns an error indicating that reset is not supported.
func (NoResetBehavior) Reset(context.Context, *sql.Tx, dogma.ProjectionResetScope) error {
	return dogma.ErrNotSupported
}
