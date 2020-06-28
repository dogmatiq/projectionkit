package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
)

// adaptor adapts an sql.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	MessageHandler

	db     *sql.DB
	key    string
	driver Driver
}

// New returns a new projection message handler that uses the given database.
//
// If d is nil, the appropriate default driver for db is used, if recognized.
func New(
	db *sql.DB,
	h MessageHandler,
	d Driver,
) (dogma.ProjectionMessageHandler, error) {
	if d == nil {
		var err error
		d, err = NewDriver(db)
		if err != nil {
			return nil, err
		}
	}

	a := &adaptor{
		MessageHandler: h,
		db:             db,
		driver:         d,
		key:            identity.Key(h),
	}

	return a, nil
}

// MustNew returns a new projection message handler that uses the given database
// or panics if unable to do so.
//
// If d is nil, the appropriate default driver for db is used, if recognized.
func MustNew(
	db *sql.DB,
	h MessageHandler,
	d Driver,
) dogma.ProjectionMessageHandler {
	a, err := New(db, h, d)
	if err != nil {
		panic(err)
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
	return a.driver.QueryVersion(ctx, a.db, a.key, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.driver.DeleteResource(ctx, a.db, a.key, r)
}

// StoreResourceVersion sets the version of the resource r to v
func (a *adaptor) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	if len(v) == 0 {
		return a.driver.DeleteResource(ctx, a.db, a.key, r)
	}

	return a.driver.StoreVersion(ctx, a.db, a.key, r, v)
}

// UpdateResourceVersion updates the version of the resource r to n without
// handling any event.
//
// If c is not the current version of r, it returns false and no update occurs.
func (a *adaptor) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	ok, err = a.driver.UpdateVersion(ctx, tx, a.key, r, c, n)
	if !ok || err != nil {
		return ok, err
	}

	return true, tx.Commit()
}

// DeleteResource removes all information about the resource r from the
// handler's data store.
func (a *adaptor) DeleteResource(ctx context.Context, r []byte) error {
	return a.driver.DeleteResource(ctx, a.db, a.key, r)
}
