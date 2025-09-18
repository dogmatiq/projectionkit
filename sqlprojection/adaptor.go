package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// adaptor adapts an sqlprojection.ProjectionMessageHandler to the
// [dogma.ProjectionMessageHandler] interface.
type adaptor struct {
	db      *sql.DB
	key     *uuidpb.UUID
	driver  Driver
	handler MessageHandler
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
		driver:  d,
		handler: h,
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
	s dogma.ProjectionEventScope,
	m dogma.Event,
) (uint64, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() // nolint:errcheck

	id := uuidpb.MustParse(s.StreamID())
	cp := s.Offset() + 1

	ok, err := a.driver.UpdateCheckpointOffset(
		ctx,
		tx,
		a.key,
		id,
		s.CheckpointOffset(),
		cp,
	)
	if err != nil {
		return 0, err
	}

	if ok {
		if err := a.handler.HandleEvent(ctx, tx, s, m); err != nil {
			return 0, err
		}
		return cp, tx.Commit()
	}

	return a.driver.QueryCheckpointOffset(
		ctx,
		a.db,
		a.key,
		id,
	)
}

// CheckpointOffset returns the offset at which the handler expects to
// resume handling events from a specific stream.
func (a *adaptor) CheckpointOffset(ctx context.Context, id string) (uint64, error) {
	return a.driver.QueryCheckpointOffset(
		ctx,
		a.db,
		a.key,
		uuidpb.MustParse(id),
	)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.db, s)
}
