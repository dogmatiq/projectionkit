package boltprojection

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
	"go.etcd.io/bbolt"
)

// adaptor adapts a boltprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	MessageHandler

	db  *bbolt.DB
	key string
}

// New returns a new Dogma projection message handler by binding a
// BoltDB-specific projection handler to a BoltDB database.
//
// If db is nil the returned handler will return an error whenever an operation
// that requires the database is performed.
func New(
	db *bbolt.DB,
	h MessageHandler,
) dogma.ProjectionMessageHandler {
	if db == nil {
		return unboundhandler.New(h)
	}

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
	defer tx.Rollback() // nolint:errcheck

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

// StoreResourceVersion sets the version of the resource r to v
func (a *adaptor) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	tx, err := a.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint:errcheck

	if err := storeVersion(ctx, tx, a.key, r, v); err != nil {
		return err
	}

	return tx.Commit()
}

// UpdateResourceVersion updates the version of the resource r to n without
// handling any event.
//
// If c is not the current version of r, it returns false and no update occurs.
func (a *adaptor) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {
	tx, err := a.db.Begin(true)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() // nolint:errcheck

	ok, err = updateVersion(ctx, tx, a.key, r, c, n)
	if !ok || err != nil {
		return ok, err
	}

	return true, tx.Commit()
}

// DeleteResource removes all information about the resource r from the
// handler's data store.
func (a *adaptor) DeleteResource(ctx context.Context, r []byte) error {
	return deleteResource(ctx, a.db, a.key, r)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.MessageHandler.Compact(ctx, a.db, s)
}
