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
	ResetFunc       func(context.Context, dogma.ProjectionResetScope) ([]types.TransactWriteItem, error)
}

// Configure declares the handler's configuration by calling methods on c.
func (h *MessageHandler) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

// HandleEvent updates the projection to reflect the occurrence of an
// [Event].
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

// Compact reduces the projection's size by removing or consolidating data.
func (h *MessageHandler) Compact(ctx context.Context, client *dynamodb.Client, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, client, s)
	}
	return nil
}

// Reset clears all projection data.
func (h *MessageHandler) Reset(ctx context.Context, s dogma.ProjectionResetScope) ([]types.TransactWriteItem, error) {
	if h.ResetFunc != nil {
		return h.ResetFunc(ctx, s)
	}
	return nil, nil
}
