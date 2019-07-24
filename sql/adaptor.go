package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
)

// adaptor is an implementation adapts an sql.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	MessageHandler

	db     *sql.DB
	key    string
	driver Driver
}

// New returns a new projection message handler that uses the given database pool.
func New(db *sql.DB, h MessageHandler) dogma.ProjectionMessageHandler {
	a := &adaptor{
		MessageHandler: h,
		db:             db,
		driver:         nil, // TODO: determine driver
		key:            identity.Key(h),
	}

	return a
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor) HandleEvent(
	ctx context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Message,
	k, v []byte,
) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := a.MessageHandler.HandleEvent(ctx, tx, s, m); err != nil {
		return err
	}

	if err := a.driver.Associate(ctx, tx, a.key, k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Recover returns the value component of a key/value association persisted
// by a call to HandleEvent().
func (a *adaptor) Recover(ctx context.Context, k []byte) (v []byte, ok bool, err error) {
	return a.driver.Recover(ctx, a.db, a.key, k)
}

// Discard informs the projection that a specific key/value association is
// no longer required.
func (a *adaptor) Discard(ctx context.Context, k []byte) error {
	return a.driver.Discard(ctx, a.db, a.key, k)
}
