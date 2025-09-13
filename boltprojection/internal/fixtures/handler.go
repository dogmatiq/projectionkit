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
// If h.HandleEventFunc is non-nil it returns h.HandleEventFunc(ctx, tx, s, m).
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

// Compact reduces the size of the projection's data.
//
// If h.CompactFunc is non-nil it returns h.CompactFunc(ctx,db,s), otherwise it
// returns nil.
func (h *MessageHandler) Compact(ctx context.Context, db *bbolt.DB, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, db, s)
	}

	return nil
}
