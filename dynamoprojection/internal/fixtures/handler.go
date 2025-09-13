package fixtures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of dynamoprojection.MessageHandler.
type MessageHandler struct {
	ConfigureFunc   func(c dogma.ProjectionConfigurer)
	HandleEventFunc func(ctx context.Context, s dogma.ProjectionEventScope, m dogma.Event) ([]types.TransactWriteItem, error)
	CompactFunc     func(context.Context, *dynamodb.Client, dogma.ProjectionCompactScope) error
}

// Configure configures the behavior of the engine as it relates to this
// handler.
//
// c provides access to the various configuration options, such as specifying
// which types of event messages are routed to this handler.
//
// If h.ConfigureFunc is non-nil, it calls h.ConfigureFunc(c).
func (h *MessageHandler) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

// HandleEvent handles a domain event message that has been routed to this
// handler.
//
// If h.HandleEventFunc is non-nil it returns h.HandleEventFunc(ctx, s, m).
func (h *MessageHandler) HandleEvent(
	ctx context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) ([]types.TransactWriteItem, error) {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, s, m)
	}

	return nil, nil
}

// Compact reduces the size of the projection's data.
//
// If h.CompactFunc is non-nil it returns h.CompactFunc(ctx,db,s), otherwise it
// returns nil.
func (h *MessageHandler) Compact(ctx context.Context, client *dynamodb.Client, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, client, s)
	}

	return nil
}
