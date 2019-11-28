package boltdb

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	bolt "go.etcd.io/bbolt"
)

// adaptor adapts a boltdb.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	MessageHandler

	db  *bolt.DB
	key string
}

// New returns a new projection message handler that uses the given database.
func New(
	db *bolt.DB,
	h MessageHandler,
) dogma.ProjectionMessageHandler {
	return &adaptor{
		MessageHandler: h,
		db:             db,
		key:            identity.Key(h),
	}
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	tx, err := a.db.Begin(true)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return false, err
	}
	defer tx.Rollback()

	ok, err := updateVersion(ctx, tx, a.key, r, c, n)
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
	return queryVersion(ctx, a.db, a.key, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return deleteResource(ctx, a.db, a.key, r)
}
