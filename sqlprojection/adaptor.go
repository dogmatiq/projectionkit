package sqlprojection

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
)

// adaptor adapts an sql.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	MessageHandler

	db  *sql.DB
	key string

	m          cosyne.Mutex
	resolved   int32 // atomic bool, fast path
	candidates []Driver
	selected   Driver
}

// New returns a new Dogma projection message handler by binding an SQL-specific
// projection handler to an SQL database.
//
// If db is nil the returned handler will return an error whenever an operation
// that requires the database is performed.
//
// By default an appropriate Driver implementation is chosen from the built-in
// drivers listed in the Drivers slice.
func New(
	db *sql.DB,
	h MessageHandler,
	options ...Option,
) dogma.ProjectionMessageHandler {
	if db == nil {
		return unboundhandler.New(h)
	}

	a := &adaptor{
		MessageHandler: h,
		db:             db,
		key:            identity.Key(h),
	}

	if len(options) == 0 {
		options = []Option{
			WithCandidateDrivers(BuiltInDrivers()...),
		}
	}

	for _, opt := range options {
		opt.applyAdaptorOption(a)
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
	return a.withTx(ctx, func(d Driver, tx *sql.Tx) (bool, error) {
		ok, err := d.UpdateVersion(ctx, tx, a.key, r, c, n)
		if !ok || err != nil {
			return ok, err
		}

		return true, a.MessageHandler.HandleEvent(ctx, tx, s, m)
	})
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	var v []byte

	return v, a.withDriver(ctx, func(d Driver) error {
		var err error
		v, err = d.QueryVersion(ctx, a.db, a.key, r)
		return err
	})
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.withDriver(ctx, func(d Driver) error {
		return d.DeleteResource(ctx, a.db, a.key, r)
	})
}

// StoreResourceVersion sets the version of the resource r to v
func (a *adaptor) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	return a.withDriver(ctx, func(d Driver) error {
		if len(v) == 0 {
			return d.DeleteResource(ctx, a.db, a.key, r)
		}

		return d.StoreVersion(ctx, a.db, a.key, r, v)
	})
}

// UpdateResourceVersion updates the version of the resource r to n without
// handling any event.
//
// If c is not the current version of r, it returns false and no update occurs.
func (a *adaptor) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (bool, error) {
	return a.withTx(ctx, func(d Driver, tx *sql.Tx) (bool, error) {
		return d.UpdateVersion(ctx, tx, a.key, r, c, n)
	})
}

// DeleteResource removes all information about the resource r from the
// handler's data store.
func (a *adaptor) DeleteResource(ctx context.Context, r []byte) error {
	return a.withDriver(ctx, func(d Driver) error {
		return d.DeleteResource(ctx, a.db, a.key, r)
	})
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.MessageHandler.Compact(ctx, a.db, s)
}

// withDriver calls fn with the driver that the adaptor should use.
func (a *adaptor) withDriver(
	ctx context.Context,
	fn func(Driver) error,
) error {
	d, err := a.driver(ctx)
	if err != nil {
		return err
	}

	return fn(d)
}

// withTx calls fn with the driver that the adaptor should use.
func (a *adaptor) withTx(
	ctx context.Context,
	fn func(Driver, *sql.Tx) (bool, error),
) (bool, error) {
	var ok bool

	err := a.withDriver(
		ctx,
		func(d Driver) error {
			tx, err := a.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			ok, err = fn(d, tx)
			if err != nil {
				return err
			}

			if ok {
				return tx.Commit()
			}

			return tx.Rollback()
		},
	)

	return ok && err == nil, err
}

// driver returns the driver that should be used by the adaptor.
func (a *adaptor) driver(ctx context.Context) (Driver, error) {
	if atomic.LoadInt32(&a.resolved) == 0 {
		// If the resolved flag is 0 then a.selected has not been populated yet.
		// We acquire the mutex to ensure we're the only goroutine attempting
		// selection.
		if err := a.m.Lock(ctx); err != nil {
			return nil, err
		}
		defer a.m.Unlock()

		// Ensure that no another goroutine selected the driver while we were
		// waiting to acquire the mutex.
		if atomic.LoadInt32(&a.resolved) == 0 {
			// If not, it's our turn to try selection.
			d, err := SelectDriver(ctx, a.db, a.candidates)
			if err != nil {
				return nil, err
			}

			a.candidates = nil
			a.selected = d
			atomic.StoreInt32(&a.resolved, 1)
		}
	}

	return a.selected, nil
}
