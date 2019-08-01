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
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	ok, err := a.driver.UpdateVersion(ctx, tx, a.key, r, c, n)
	if !ok || err != nil {
		return ok, err
	}

	if err := a.MessageHandler.HandleEvent(ctx, tx, s, m); err != nil {
		return false, err
	}

	return true, tx.Commit()
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return a.driver.ResourceVersion(ctx, a.db, a.key, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.driver.CloseResource(ctx, a.db, a.key, r)
}
