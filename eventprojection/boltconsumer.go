package eventprojection

import (
	"context"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

// boltConsumer is an implementation of Consumer for BoltDB-based event
// projections.
type boltConsumer struct {
	db        *bbolt.DB
	key       []byte
	unmarshal func([]byte) (Event, error)
}

// OpenAt returns a cursor used to consume events beginning at a specific
// offset.
func (c *boltConsumer) OpenAt(
	ctx context.Context,
	streamID string,
	offset uint64,
) (Cursor, error) {
	return &boltCursor{
		c.db,
		c.key,
		c.unmarshal,
		streamID,
		offset,
	}, nil
}

// boltCursor is an implementation of Cursor for BoltDB-based event projections.
type boltCursor struct {
	db        *bbolt.DB
	key       []byte
	unmarshal func([]byte) (Event, error)
	streamID  string
	offset    uint64
}

// Next returns the next event in the stream.
func (c *boltCursor) Next(ctx context.Context) (Event, bool, error) {
	if c.db == nil {
		return nil, false, errors.New("cursor is closed")
	}

	tx, err := c.db.Begin(false)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	b, ok := loadBuckets(tx, c.key)
	if !ok {
		return nil, false, nil
	}

	sb, ok := b.LoadForStream(c.streamID)
	if !ok {
		return nil, false, nil
	}

	data := sb.EventContainer.Get(marshalUint64(c.offset))
	if data == nil {
		if sb.StreamRoot.Get(expireAtKey) != nil {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf(
			"can not load event at offset %d",
			c.offset,
		)
	}

	ev, err := c.unmarshal(data)
	if err != nil {
		return nil, false, err
	}

	c.offset++
	return ev, true, nil
}

// Close closes the cursor.
func (c *boltCursor) Close() error {
	c.db = nil
	return nil
}
