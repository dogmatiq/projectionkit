package dynamoprojection

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a specialization of dogma.ProjectionMessageHandler that
// persists to AWS DynamoDB.
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
	// The changes to be made are returned as a slice of transaction items,
	// which may be empty. The items are applied to DynamoDB in a single
	// transaction.
	HandleEvent(ctx context.Context, s dogma.ProjectionEventScope, m dogma.Event) ([]types.TransactWriteItem, error)

	// Compact reduces the projection's size by removing or consolidating data.
	//
	// The handler might delete obsolete entries, merge fine-grained data into
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
	Compact(ctx context.Context, client *dynamodb.Client, s dogma.ProjectionCompactScope) error

	// Reset clears all projection data.
	//
	// The changes to be made are returned as a slice of transaction items,
	// which may be empty. The items are applied to DynamoDB in a single
	// transaction.
	//
	// Not all projections can be reset. Embed [NoResetBehavior] in the handler
	// to indicate that reset is not supported.
	Reset(ctx context.Context, s dogma.ProjectionResetScope) ([]types.TransactWriteItem, error)
}

// NoCompactBehavior is an embeddable type for [MessageHandler] implementations
// that don't require compaction.
//
// Embed this type in a [MessageHandler] when projection data doesn't grow
// unbounded or when an external system handles compaction.
type NoCompactBehavior struct{}

// Compact returns nil.
func (NoCompactBehavior) Compact(context.Context, *dynamodb.Client, dogma.ProjectionCompactScope) error {
	return nil
}

// NoResetBehavior is an embeddable type for [MessageHandler] implementations
// that don't support resetting their state.
//
// Embed this type in a [MessageHandler] when resetting projection data isn't
// feasible or required.
type NoResetBehavior struct{}

// Reset returns an error indicating that reset is not supported.
func (NoResetBehavior) Reset(context.Context, dogma.ProjectionResetScope) ([]types.TransactWriteItem, error) {
	return nil, dogma.ErrNotSupported
}
