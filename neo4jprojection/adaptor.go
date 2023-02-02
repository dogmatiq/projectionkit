package neo4jprojection

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
	"github.com/dogmatiq/projectionkit/resource"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// adaptor adapts a boltprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	db      neo4j.DriverWithContext
	handler MessageHandler
	repo    *ResourceRepository
}

// New returns a new Dogma projection message handler by binding a
// BoltDB-specific projection handler to a BoltDB database.
//
// If db is nil the returned handler will return an error whenever an operation
// that requires the database is performed.
func New(
	db neo4j.DriverWithContext,
	h MessageHandler,
	occTable string,
) dogma.ProjectionMessageHandler {
	if db == nil {
		return unboundhandler.New(h)
	}

	return &adaptor{
		db:      db,
		handler: h,
		repo: NewResourceRepository(
			db,
			identity.Key(h),
			occTable,
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
	m dogma.Message,
) (bool, error) {
	return a.repo.UpdateResourceVersionFn(
		ctx,
		r, c, n,
		func(ctx context.Context, tx neo4j.ExplicitTransaction) (bool, error) {
			err := a.handler.HandleEvent(ctx, tx, s, m)
			if err != nil {
				return false, err
			}
			return true, nil
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

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *adaptor) TimeoutHint(m dogma.Message) time.Duration {
	return a.handler.TimeoutHint(m)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.db, s)
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor) ResourceRepository() resource.Repository {
	return a.repo
}
