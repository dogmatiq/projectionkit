package fixtures

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of dynamoprojection.MessageHandler.
type MessageHandler struct {
	ConfigureFunc   func(c dogma.ProjectionConfigurer)
	HandleEventFunc func(ctx context.Context, s dogma.ProjectionEventScope, m dogma.Message) ([]types.TransactWriteItem, error)
	TimeoutHintFunc func(m dogma.Message) time.Duration
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
	m dogma.Message,
) ([]types.TransactWriteItem, error) {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, s, m)
	}

	return nil, nil
}

// TimeoutHint returns a duration that is suitable for computing a deadline for
// the handling of the given message by this handler.
//
// If h.TimeoutHintFunc is non-nil it returns h.TimeoutHintFunc(m), otherwise it
// returns 0.
func (h *MessageHandler) TimeoutHint(m dogma.Message) time.Duration {
	if h.TimeoutHintFunc != nil {
		return h.TimeoutHintFunc(m)
	}

	return 0
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
