package fixtures

import (
	"context"

	"github.com/dogmatiq/dogma"
	"go.etcd.io/bbolt"
)

// MessageHandler is a test implementation of boltdb.MessageHandler.
type MessageHandler struct {
	ConfigureFunc   func(c dogma.ProjectionConfigurer)
	HandleEventFunc func(context.Context, *bbolt.Tx, dogma.ProjectionEventScope, dogma.Event) error
	CompactFunc     func(context.Context, *bbolt.DB, dogma.ProjectionCompactScope) error
	ResetFunc       func(context.Context, *bbolt.Tx, dogma.ProjectionResetScope) error
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
	tx *bbolt.Tx,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, tx, s, m)
	}
	return nil
}

// Compact reduces the projection's size by removing or consolidating data.
func (h *MessageHandler) Compact(ctx context.Context, db *bbolt.DB, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, db, s)
	}
	return nil
}

// Reset clears all projection data.
func (h *MessageHandler) Reset(ctx context.Context, tx *bbolt.Tx, s dogma.ProjectionResetScope) error {
	if h.ResetFunc != nil {
		return h.ResetFunc(ctx, tx, s)
	}
	return nil
}
