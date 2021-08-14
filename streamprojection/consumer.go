package streamprojection

import (
	"context"
)

// Consumer is used to consume state and updates from a stream projection.
type Consumer struct {
}

// Stream returns the stream with the given ID.
func (c *Consumer) Stream(id string) *Stream {
	panic("not implemented")
}

// Stream represents a specfic stream produced by a projection.
type Stream struct {
	id string
}

// ID returns the ID of the stream.
func (s *Stream) ID() string {
	return s.id
}

// Open returns a cursor used to consume events from the stream starting at the
// most recent snapshot.
func (s *Stream) Open(ctx context.Context) (*Cursor, Snapshot, error) {
	panic("not implemented")
}

// Resume returns a cursor used to resuming consuming events that occur after a
// specific version.
//
// If ok is false the version is older than earliest available event; use Open()
// to obtain a cursor that starts from the most recent snapshot.
func (s *Stream) Resume(ctx context.Context, version int64) (_ *Cursor, ok bool, _ error) {
	panic("not implemented")
}

// A Cursor is used to consume events from a stream.
type Cursor struct {
}

// Version returns the version that the cursor points to.
func (c *Cursor) Version() int64 {
	panic("not implemented")
}

// Next returns the next event in the stream.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream, ctx is canceled.
//
// If the stream is closed before or during a call to Next(), ok is false.
// Future calls to Next() will return the same result.
func (c *Cursor) Next(ctx context.Context) (_ Event, ok bool, _ error) {
	panic("not implemented")
}

// Close discards the cursor.
func (c *Cursor) Close() error {
	return nil
}
