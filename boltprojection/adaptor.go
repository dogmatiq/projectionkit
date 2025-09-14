package boltprojection

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
)

// adaptor adapts a boltprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	db      *bbolt.DB
	key     *uuidpb.UUID
	handler MessageHandler
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
	return &adaptor{
		db:      db,
		key:     identity.Key(h),
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
	id := []byte(s.StreamID())
	var cp uint64

	return cp, a.db.Update(func(tx *bbolt.Tx) error {
		b, err := makeHandlerBucket(tx, a.key)
		if err != nil {
			// CODE COVERAGE: This branch can not be easily covered without
			// somehow breaking the BoltDB connection or the database file in
			// some way.
			return err
		}

		cp, err = getCheckpointOffset(b, id)
		if err != nil {
			return err
		}

		if s.CheckpointOffset() != cp {
			return nil
		}

		if err := a.handler.HandleEvent(ctx, tx, s, m); err != nil {
			return err
		}

		cp = s.Offset() + 1

		return b.Put(
			id,
			binary.BigEndian.AppendUint64(nil, cp),
		)
	})
}

// CheckpointOffset returns the offset at which the handler expects to
// resume handling events from a specific stream.
func (a *adaptor) CheckpointOffset(_ context.Context, id string) (uint64, error) {
	var cp uint64

	return cp, a.db.View(func(tx *bbolt.Tx) (err error) {
		if b := handlerBucket(tx, a.key); b != nil {
			cp, err = getCheckpointOffset(b, []byte(id))
		}
		return err
	})
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.db, s)
}

var (
	// topBucket is the bucket at the root level that contains all data related
	// to projection checkpoint offsets.
	topBucket = []byte("projection_checkpoint")
)

// makeHandlerBucket creates a bucket for the given handler key if it has not
// been created yet.
//
// This function returns an error it tx is not writable.
func makeHandlerBucket(tx *bbolt.Tx, hk *uuidpb.UUID) (*bbolt.Bucket, error) {
	tb, err := tx.CreateBucketIfNotExists(topBucket)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return nil, err
	}

	return tb.CreateBucketIfNotExists(hk.AsBytes())
}

// handlerBucket retrieves a bucket for the given handler key. If a bucket with
// the given handler key does not exist, this function returns nil.
func handlerBucket(tx *bbolt.Tx, hk *uuidpb.UUID) *bbolt.Bucket {
	tb := tx.Bucket(topBucket)
	if tb == nil {
		return nil
	}

	return tb.Bucket(hk.AsBytes())
}

// getCheckpointOffset retrieves the checkpoint offset for a specific stream ID.
func getCheckpointOffset(b *bbolt.Bucket, id []byte) (uint64, error) {
	switch data := b.Get(id); len(data) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(data), nil
	default:
		return 0, fmt.Errorf("malformed checkpoint: expected 8 bytes, got %d", len(data))
	}
}
