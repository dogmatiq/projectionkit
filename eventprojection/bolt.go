package eventprojection

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
)

type boltHandler struct {
	handler MessageHandler
	key     []byte
}

var (
	rootBucketName = []byte("event_projection")
	retainUntilKey = []byte("retain_until")
)

func NewBolt(db *bbolt.DB, h MessageHandler) dogma.ProjectionMessageHandler {
	return boltprojection.New(db, &boltHandler{
		h,
		[]byte(identity.Key(h)),
	})
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (h *boltHandler) Configure(c dogma.ProjectionConfigurer) {
	h.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (h *boltHandler) HandleEvent(
	ctx context.Context,
	tx *bbolt.Tx,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) error {
	sc := &scope{
		ProjectionEventScope: s,
	}

	h.handler.HandleEvent(sc, m)

	if len(sc.changes) == 0 {
		return nil
	}

	// Get the root bucket for storing event projections.
	rootBucket, err := tx.CreateBucketIfNotExists(rootBucketName)
	if err != nil {
		return err
	}

	// Get the bucket used to store information for this specific projection.
	handlerBucket, err := rootBucket.CreateBucketIfNotExists(h.key)
	if err != nil {
		return err
	}

	for stream, changes := range sc.changes {
		// Get the bucket used to store data about this specific stream.
		streamBucket, err := handlerBucket.CreateBucketIfNotExists([]byte(stream))
		if err != nil {
			return err
		}

		// Get the
		eventsBucket, err := streamBucket.CreateBucketIfNotExists([]byte(stream))
		if err != nil {
			return err
		}

		for _, ev := range changes.Events {
			if err := appendEvent(eventsBucket, ev); err != nil {
				return err
			}
		}

		if changes.Closed {
			data, err := changes.RetainUntil.MarshalBinary()
			if err != nil {
				return err
			}

			if err := streamBucket.Put(retainUntilKey, data); err != nil {
				return err
			}
		}
	}

	return nil
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (h *boltHandler) TimeoutHint(m dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data.
func (h *boltHandler) Compact(
	ctx context.Context,
	db *bbolt.DB,
	s dogma.ProjectionCompactScope,
) error {
	return nil
}

// appendEvent appends an event to an event bucket.
func appendEvent(b *bbolt.Bucket, ev Event) error {
	data, err := ev.MarshalBinary()
	if err != nil {
		return err
	}

	id, err := b.NextSequence()
	if err != nil {
		return err
	}

	var key [4]byte
	binary.BigEndian.PutUint64(key[:], id)

	return b.Put(key[:], data)
}
