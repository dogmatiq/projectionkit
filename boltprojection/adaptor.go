package boltprojection

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
)

// adaptor wraps a [MessageHandler] to provide the
// [dogma.ProjectionMessageHandler] interface.
type adaptor struct {
	DB      *bbolt.DB
	Handler MessageHandler

	handlerKey [16]byte
}

// New returns a new [dogma.ProjectionMessageHandler] that binds a
// BoltDB-specific [MessageHandler] to a BoltDB database.
func New(
	db *bbolt.DB,
	handler MessageHandler,
) dogma.ProjectionMessageHandler {
	return &adaptor{
		DB:      db,
		Handler: handler,

		handlerKey: identity.Key(handler),
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
	id := uuidpb.MustParseAsByteArray(s.StreamID())
	var cp uint64

	return cp, a.DB.Update(func(tx *bbolt.Tx) error {
		b, err := makeBucketForHandler(tx, a.handlerKey)
		if err != nil {
			return err
		}

		cp, err = getCheckpointOffset(b, id)
		if err != nil {
			return err
		}

		if s.CheckpointOffset() != cp {
			return nil
		}

		if err := a.Handler.HandleEvent(ctx, tx, s, m); err != nil {
			return err
		}

		cp = s.Offset() + 1

		return b.Put(
			id[:],
			binary.BigEndian.AppendUint64(nil, cp),
		)
	})
}

func (a *adaptor) CheckpointOffset(_ context.Context, id string) (uint64, error) {
	var cp uint64

	return cp, a.DB.View(func(tx *bbolt.Tx) (err error) {
		if b := bucketForHandler(tx, a.handlerKey); b != nil {
			cp, err = getCheckpointOffset(b, uuidpb.MustParseAsByteArray(id))
		}
		return err
	})
}

func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.Handler.Compact(ctx, a.DB, s)
}

func (a *adaptor) Reset(ctx context.Context, s dogma.ProjectionResetScope) error {
	return a.DB.Update(func(tx *bbolt.Tx) error {
		if err := a.Handler.Reset(ctx, tx, s); err != nil {
			return err
		}

		return deleteBucketForHandler(tx, a.handlerKey)
	})
}

var (
	// checkpointBucket is the bucket at the root level that contains all data
	// related to projection checkpoint offsets.
	checkpointBucket = []byte("projection_checkpoint")
)

// makeBucketForHandler returns the bucket for storing checkpoint offsets for
// the handler with the given key, creating it if it does not already exist.
//
// It returns an error if the transaction is not writable.
func makeBucketForHandler(tx *bbolt.Tx, hk [16]byte) (*bbolt.Bucket, error) {
	b, err := tx.CreateBucketIfNotExists(checkpointBucket)
	if err != nil {
		return nil, err
	}

	return b.CreateBucketIfNotExists(hk[:])
}

// bucketForHandler returns the bucket for storing checkpoint offsets for the
// handler with the given key.
//
// It returns nil if the bucket does not exist.
func bucketForHandler(tx *bbolt.Tx, hk [16]byte) *bbolt.Bucket {
	b := tx.Bucket(checkpointBucket)
	if b == nil {
		return nil
	}

	return b.Bucket(hk[:])
}

// deleteBucketForHandler deletes the bucket for storing checkpoint offsets for
// the handler with the given key.
//
// It does nothing if the bucket does not exist.
func deleteBucketForHandler(tx *bbolt.Tx, hk [16]byte) error {
	b := tx.Bucket(checkpointBucket)
	if b == nil {
		return nil
	}

	err := b.DeleteBucket(hk[:])

	if err == errors.ErrBucketNotFound {
		return nil
	}

	return err
}

// getCheckpointOffset retrieves the checkpoint offset for a specific stream ID.
//
// b is a handler-specific bucket returned by [makeBucketForHandler] or
// [bucketForHandler].
func getCheckpointOffset(b *bbolt.Bucket, id [16]byte) (uint64, error) {
	switch data := b.Get(id[:]); len(data) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(data), nil
	default:
		return 0, fmt.Errorf("malformed checkpoint: expected 8 bytes, got %d", len(data))
	}
}
