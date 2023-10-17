package sqlprojection

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
	"github.com/dogmatiq/projectionkit/resource"
)

// adaptor adapts an sqlprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	db      *sql.DB
	handler MessageHandler
	repo    *ResourceRepository

	schemaCreated  atomic.Bool
	schemaCreatedM sync.Mutex
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
		db:      db,
		handler: h,
		repo: NewResourceRepository(
			db,
			identity.Key(h),
			options...,
		),
	}

	for _, opt := range options {
		opt.applyToAdaptor(a)
	}

	return a
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	if err := a.initSchema(ctx); err != nil {
		return false, err
	}

	return a.repo.UpdateResourceVersionFn(
		ctx,
		r, c, n,
		func(ctx context.Context, tx *sql.Tx) error {
			return a.handler.HandleEvent(ctx, tx, s, m)
		},
	)
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	repo, err := a.ResourceRepository(ctx)
	if err != nil {
		return nil, err
	}
	return repo.ResourceVersion(ctx, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	repo, err := a.ResourceRepository(ctx)
	if err != nil {
		return err
	}
	return repo.DeleteResource(ctx, r)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *adaptor) TimeoutHint(m dogma.Message) time.Duration {
	return a.handler.TimeoutHint(m)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	if err := a.initSchema(ctx); err != nil {
		return err
	}
	return a.handler.Compact(ctx, a.db, s)
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor) ResourceRepository(ctx context.Context) (resource.Repository, error) {
	if err := a.initSchema(ctx); err != nil {
		return nil, err
	}
	return a.repo, nil
}

// initSchema creates the SQL schema required by the projection if it has not
// already been created.
func (a *adaptor) initSchema(ctx context.Context) error {
	if a.schemaCreated.Load() {
		return nil
	}

	a.schemaCreatedM.Lock()
	defer a.schemaCreatedM.Unlock()

	if a.schemaCreated.Load() {
		return nil
	}

	if err := a.repo.withDriver(ctx, func(d Driver) error {
		return d.CreateSchema(ctx, a.db)
	}); err != nil {
		return err
	}

	if err := a.handler.CreateSchema(ctx, a.db); err != nil {
		return err
	}

	a.schemaCreated.Store(true)

	return nil
}
