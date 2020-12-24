package patchprojection

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
)

type boltAdaptor struct {
	MessageHandler

	key string

	loaded uint32 // atomic bool
	m      cosyne.Mutex
	state  State
}

// NewBolt returns a BoltDB-based projection message handler that manages the
// persistence for a patchprojection.MessageHandler.
func NewBolt(h MessageHandler) (boltprojection.MessageHandler, *Stream) {
	a := &boltAdaptor{
		MessageHandler: h,
		key:            identity.Key(h),
	}

	s := &Stream{}

	return a, s
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *boltAdaptor) HandleEvent(
	ctx context.Context,
	tx *bbolt.Tx,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) error {
	if err := a.loadOnce(ctx, tx); err != nil {
		return err
	}

	patches := a.MessageHandler.HandleEvent(s, m)
	if len(patches) == 0 {
		return nil
	}

	data, err := a.MessageHandler.MarshalState()
	if err != nil {
		return err
	}

	b, err := makeHandlerBucket(tx, a.key)
	if err != nil {
		return err
	}

	if err := b.Put(stateKey, data); err != nil {
		return err
	}
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *boltAdaptor) TimeoutHint(m dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data.
func (a *boltAdaptor) Compact(
	ctx context.Context,
	db *bbolt.DB,
	s dogma.ProjectionCompactScope,
) error {
	return nil
}

// loadOnce reads and unmarshals the existing projection state if it has not
// already been loaded.
func (a *boltAdaptor) loadOnce(ctx context.Context, tx *bbolt.Tx) error {
	if atomic.LoadUint32(&a.loaded) != 0 {
		return nil
	}

	if err := a.m.Lock(ctx); err != nil {
		return err
	}
	defer a.m.Unlock()

	if a.loaded != 0 {
		return nil
	}

	var data []byte
	if b := handlerBucket(tx, a.key); b != nil {
		data = b.Get(stateKey)
	}

	state, err := a.MessageHandler.UnmarshalState(data)
	if err != nil {
		return err
	}

	a.state = state
	atomic.StoreUint32(&a.loaded, 1)
}

var (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = []byte("projection_patch")

	// stateKey is the bucket key used to store the projection's state.
	stateKey = []byte("state")
)

// makeHandlerBucket creates a bucket for the given handler key if it has not
// been created yet.
//
// This function returns an error it tx is not writable.
func makeHandlerBucket(tx *bbolt.Tx, hk string) (*bbolt.Bucket, error) {
	tb, err := tx.CreateBucketIfNotExists(topBucket)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return nil, err
	}

	return tb.CreateBucketIfNotExists([]byte(hk))
}

// handlerBucket retrieves a bucket for the given handler key. If a bucket with
// the given handler key does not exist, this function returns nil.
func handlerBucket(tx *bbolt.Tx, hk string) *bbolt.Bucket {
	tb := tx.Bucket(topBucket)
	if tb == nil {
		return nil
	}

	return tb.Bucket([]byte(hk))
}
