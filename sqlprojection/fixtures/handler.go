package fixtures

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of sql.MessageHandler.
type MessageHandler struct {
	ConfigureFunc    func(dogma.ProjectionConfigurer)
	HandleEventFunc  func(context.Context, *sql.Tx, dogma.ProjectionEventScope, dogma.Message) error
	TimeoutHintFunc  func(dogma.Message) time.Duration
	CompactFunc      func(context.Context, *sql.DB, dogma.ProjectionCompactScope) error
	CreateSchemaFunc func(context.Context, *sql.DB) error
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
	tx *sql.Tx,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, tx, s, m)
	}

	return nil
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
func (h *MessageHandler) Compact(ctx context.Context, db *sql.DB, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, db, s)
	}

	return nil
}

// CreateSchema creates the SQL schema required by the projection.
//
// If h.CreateSchemaFunc is non-nil it returns h.CreateSchemaFunc(ctx,db),
// otherwise it returns nil.
func (h *MessageHandler) CreateSchema(ctx context.Context, db *sql.DB) error {
	if h.CreateSchemaFunc != nil {
		return h.CreateSchemaFunc(ctx, db)
	}

	return nil
}
