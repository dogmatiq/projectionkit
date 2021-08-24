package eventprojection

import (
	"encoding/binary"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
)

// NewBolt returns a new Dogma projection message handler by binding an
// event-based projection handler to a BoltDB database.
//
// If db is nil the returned handler will return an error whenever an operation
// that requires the database is performed.
func NewBolt(
	db *bbolt.DB,
	h MessageHandler,
) (dogma.ProjectionMessageHandler, Consumer) {
	k := []byte(identity.Key(h))
	a := &boltAdaptor{h, k}
	c := &boltConsumer{db, k, h.UnmarshalEvent}
	return boltprojection.New(db, a), c
}

var (
	// rootBucket is the name for the bucket that contains all data for event
	// projections.
	//
	// It contains one sub-bucket for each projection handler, referred to as
	// the "handler root". They are keyed by handler identity key.
	rootBucket = []byte("event_projections")

	// expiryIndexBucket is the name for a bucket that indexes streams by their
	// expiry time. It is a sub-bucket of a "handler root".
	//
	// It contains one sub-bucket for each unique expiry timestamp. They are
	// keyed by the 64-bit big-endian binary unix representation of expiry time.
	//
	// Each sub-bucket contains keys that are the IDs of streams due to expire
	// at that time. The values are empty.
	expiryIndexBucket = []byte("expiry_index")

	// streamContainerBucket is the name for a bucket that contains information
	// about the streams produced by a specific handler. It is a sub-bucket of a
	// "handler root".
	//
	// It contains one sub-bucket for each stream that the projection has
	// produced, referred to as the "stream root". They are keyed by stream ID.
	streamContainerBucket = []byte("streams")

	// expireAtKey is the key used to store a stream's expiry time within a
	// "stream root".
	//
	// Presence of this key indicates that the stream has been closed. The value
	// is the big-endian binary representation of the 64-bit unix time at which
	// the stream expires.
	expireAtKey = []byte("expire_at")

	// eventContainerBucket is the name for a bucket that contains the events
	// within a stream. It is a sub-bucket of a "stream root"
	//
	// Each value in the bucket the binary representation of an event. Events
	// are keyed by the big-endian representation their ID, which is a
	// monotonically increasing, 1-based uint64.
	eventContainerBucket = []byte("events")
)

// buckets encapsulates the BoltDB buckets used to store events.
type buckets struct {
	Root            *bbolt.Bucket
	HandlerRoot     *bbolt.Bucket
	ExpiryIndex     *bbolt.Bucket
	StreamContainer *bbolt.Bucket
}

// createBuckets creates and returns the buckets used to store data for the
// projection with the given handler key.
func createBuckets(tx *bbolt.Tx, hk []byte) (buckets, error) {
	root, err := tx.CreateBucketIfNotExists(rootBucket)
	if err != nil {
		return buckets{}, err
	}

	handlerRoot, err := root.CreateBucketIfNotExists(hk)
	if err != nil {
		return buckets{}, err
	}

	expiryIndex, err := handlerRoot.CreateBucketIfNotExists(expiryIndexBucket)
	if err != nil {
		return buckets{}, err
	}

	streamContainer, err := handlerRoot.CreateBucketIfNotExists(streamContainerBucket)
	if err != nil {
		return buckets{}, err
	}

	return buckets{
		root,
		handlerRoot,
		expiryIndex,
		streamContainer,
	}, nil
}

// loadBuckets returns the buckets used to store data for the projection with
// the given handler key.
func loadBuckets(tx *bbolt.Tx, hk []byte) (buckets, bool) {
	root := tx.Bucket(rootBucket)
	if root == nil {
		return buckets{}, false
	}

	handlerRoot := root.Bucket(hk)
	if handlerRoot == nil {
		return buckets{}, false
	}

	expiryIndex := handlerRoot.Bucket(expiryIndexBucket)
	if expiryIndex == nil {
		return buckets{}, false
	}

	streamContainer := handlerRoot.Bucket(streamContainerBucket)
	if streamContainer == nil {
		return buckets{}, false
	}

	return buckets{
		root,
		handlerRoot,
		expiryIndex,
		streamContainer,
	}, true
}

// CreateForStream creates the buckets used to store information for a single
// stream.
func (b buckets) CreateForStream(streamID string) (streamBuckets, error) {
	streamKey := []byte(streamID)

	streamRoot, err := b.StreamContainer.CreateBucketIfNotExists(streamKey)
	if err != nil {
		return streamBuckets{}, err
	}

	eventContainer, err := streamRoot.CreateBucketIfNotExists(eventContainerBucket)
	if err != nil {
		return streamBuckets{}, err
	}

	return streamBuckets{
		streamKey,
		streamRoot,
		eventContainer,
	}, nil
}

// LoadForStream returns the buckets used to store information for a single
// stream, or false if they do not exist.
func (b buckets) LoadForStream(streamID string) (streamBuckets, bool) {
	streamKey := []byte(streamID)

	streamRoot := b.StreamContainer.Bucket(streamKey)
	if streamRoot == nil {
		return streamBuckets{}, false
	}

	eventContainer := streamRoot.Bucket(eventContainerBucket)
	if eventContainer == nil {
		return streamBuckets{}, false
	}

	return streamBuckets{
		streamKey,
		streamRoot,
		eventContainer,
	}, true
}

// streamBuckets encapsulates the BoltDB buckets used to store a specific
// stream.
type streamBuckets struct {
	StreamKey      []byte
	StreamRoot     *bbolt.Bucket
	EventContainer *bbolt.Bucket
}

// marshalTime marshals a time to a big-endian 64-bit unix timestamp
// representation.
func marshalTime(t time.Time) []byte {
	u := t.Unix()

	if u < 0 {
		// We rely on being able to compare timestamps while in binary form, so
		// don't allow encoding negative values. Such timestamps are always
		// older than "now" anyway.
		u = 0
	}

	return marshalUint64(uint64(u))
}

// marshalUint64 returns the big-endian binary representation of v.
func marshalUint64(v uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, v)
	return data
}
