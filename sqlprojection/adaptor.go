package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/projectionkit/internal/identity"
)

// adaptor adapts an sqlprojection.ProjectionMessageHandler to the
// [dogma.ProjectionMessageHandler] interface.
type adaptor struct {
	DB      *sql.DB
	Driver  Driver
	Handler MessageHandler

	handlerKey [16]byte
}

// New returns a new [dogma.ProjectionMessageHandler] that binds an
// SQL-specific [MessageHandler] to an SQL database.
func New(
	db *sql.DB,
	d Driver,
	h MessageHandler,
) dogma.ProjectionMessageHandler {
	return &adaptor{
		DB:      db,
		Driver:  d,
		Handler: h,

		handlerKey: identity.Key(h),
	}
}

func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.Handler.Configure(c)
}

func (a *adaptor) HandleEvent(
	ctx context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) (uint64, error) {
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() // nolint:errcheck

	id := uuidpb.MustParseAsBytes(s.StreamID())
	cp := s.Offset() + 1

	ok, err := a.Driver.UpdateCheckpointOffset(
		ctx,
		tx,
		a.handlerKey[:],
		id,
		s.CheckpointOffset(),
		cp,
	)
	if err != nil {
		return 0, err
	}

	if ok {
		if err := a.Handler.HandleEvent(ctx, tx, s, m); err != nil {
			return 0, err
		}
		return cp, tx.Commit()
	}

	return a.Driver.QueryCheckpointOffset(
		ctx,
		a.DB,
		a.handlerKey[:],
		id,
	)
}

func (a *adaptor) CheckpointOffset(ctx context.Context, id string) (uint64, error) {
	return a.Driver.QueryCheckpointOffset(
		ctx,
		a.DB,
		a.handlerKey[:],
		uuidpb.MustParseAsBytes(id),
	)
}

func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.Handler.Compact(ctx, a.DB, s)
}

func (a *adaptor) Reset(ctx context.Context, s dogma.ProjectionResetScope) error {
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint:errcheck

	if err := a.Handler.Reset(ctx, tx, s); err != nil {
		return err
	}

	if err := a.Driver.DeleteCheckpointOffsets(
		ctx,
		tx,
		a.handlerKey[:],
	); err != nil {
		return err
	}

	return tx.Commit()
}
