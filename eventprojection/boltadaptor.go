package eventprojection

import (
	"bytes"
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"go.etcd.io/bbolt"
	"go.uber.org/multierr"
)

// boltAdaptor adapts an eventprojection.MessageHandler to the
// dogma.ProjectionMessageHandler interface.
type boltAdaptor struct {
	handler MessageHandler
	key     []byte
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (a *boltAdaptor) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *boltAdaptor) HandleEvent(
	ctx context.Context,
	tx *bbolt.Tx,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) error {
	sc := &scope{
		ProjectionEventScope: s,
	}

	a.handler.HandleEvent(sc, m)

	if len(sc.changes) == 0 {
		return nil
	}

	b, err := createBuckets(tx, a.key)
	if err != nil {
		return err
	}

	for streamID, changes := range sc.changes {
		if err := a.applyChanges(b, streamID, changes); err != nil {
			return err
		}
	}

	return nil
}

// applyChanges updates the database to reflect the changes to a single stream.
func (a *boltAdaptor) applyChanges(
	b buckets,
	streamID string,
	changes *streamChanges,
) error {
	sb, err := b.CreateForStream(streamID)
	if err != nil {
		return err
	}

	for _, ev := range changes.Events {
		if err := a.appendEvent(sb.EventContainer, ev); err != nil {
			return err
		}
	}

	if changes.ExpireAt != nil {
		return closeStream(b, sb, *changes.ExpireAt)
	}

	if len(changes.Events) != 0 {
		return reopenStream(b, sb)
	}

	return nil
}

// appendEvent stores an event with the next available sequence ID.
func (a *boltAdaptor) appendEvent(eventContainer *bbolt.Bucket, ev Event) error {
	data, err := a.handler.MarshalEvent(ev)
	if err != nil {
		return err
	}

	// Theoretically improve performance for append-heavy write loads.
	eventContainer.FillPercent = 1

	id, err := eventContainer.NextSequence()
	if err != nil {
		return err
	}

	return eventContainer.Put(
		marshalUint64(id-1), // store by offset, not by ID
		data,
	)
}

// closeStream marks a stream as closed by storing its expiry timestamp in the
// stream root and the expiry index.
func closeStream(
	b buckets,
	sb streamBuckets,
	expireAt time.Time,
) error {
	expireAtBinary := marshalTime(expireAt)

	if err := sb.StreamRoot.Put(expireAtKey, expireAtBinary); err != nil {
		return err
	}

	entry, err := b.ExpiryIndex.CreateBucketIfNotExists(expireAtBinary)
	if err != nil {
		return err
	}

	// Theoretically improve performance for append-heavy write loads.
	entry.FillPercent = 1

	return entry.Put(sb.StreamKey, nil)
}

// reopenStream marks a stream as open by removing its expiry timestamp from the
// stream root and the expiry index.
func reopenStream(
	b buckets,
	sb streamBuckets,
) error {
	expireAtBinary := sb.StreamRoot.Get(expireAtKey)
	if expireAtBinary == nil {
		return nil
	}

	if err := sb.StreamRoot.Delete(expireAtKey); err != nil {
		return err
	}

	entry := b.ExpiryIndex.Bucket(expireAtBinary)
	if entry == nil {
		return nil
	}

	return entry.Delete(sb.StreamKey)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *boltAdaptor) TimeoutHint(m dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data by removing expired
// streams.
func (a *boltAdaptor) Compact(
	ctx context.Context,
	db *bbolt.DB,
	s dogma.ProjectionCompactScope,
) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, ok := loadBuckets(tx, a.key)
	if !ok {
		return nil
	}

	if err := expireStreams(ctx, s, b); err != nil && err != ctx.Err() {
		// Only abort if the error was something other than the context
		// cancelation, otherwise we still want to commit the transaction.
		return err
	}

	return multierr.Append(
		ctx.Err(),
		tx.Commit(),
	)
}

// expireStreams deletes expired streams from the database.
func expireStreams(
	ctx context.Context,
	s dogma.ProjectionCompactScope,
	b buckets,
) error {
	now := marshalTime(s.Now())
	entries := b.ExpiryIndex.Cursor()

	for {
		// Bail if the context is canceled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Find the oldest entry in the index, bail if it's in the future.
		expireAt, _ := entries.First()
		if bytes.Compare(expireAt, now) > 0 {
			break
		}

		streamKeys := b.ExpiryIndex.Bucket(expireAt).Cursor()

		// Delete each stream recorded in this index entry.
		for streamKey, _ := streamKeys.First(); streamKey != nil; streamKey, _ = streamKeys.Next() {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if err := b.StreamContainer.DeleteBucket(streamKey); err != nil {
				return err
			}

			if err := streamKeys.Delete(); err != nil {
				return err
			}

			s.Log("deleted expired '%s' stream", streamKey)
		}

		// Delete the index entry itself.
		if err := b.ExpiryIndex.DeleteBucket(expireAt); err != nil {
			return err
		}
	}

	return nil
}
