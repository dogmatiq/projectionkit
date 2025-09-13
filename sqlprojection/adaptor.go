package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/resource"
)

// adaptor adapts an sqlprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	db      *sql.DB
	handler MessageHandler
	repo    *ResourceRepository
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
	d Driver,
	h MessageHandler,
) dogma.ProjectionMessageHandler {
	return &adaptor{
		db:      db,
		handler: h,
		repo: NewResourceRepository(
			db,
			d,
			identity.Key(h),
		),
	}
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
	m dogma.Event,
) (bool, error) {
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
	return a.repo.ResourceVersion(ctx, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.repo.DeleteResource(ctx, r)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.db, s)
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor) ResourceRepository(context.Context) (resource.Repository, error) {
	return a.repo, nil
}
