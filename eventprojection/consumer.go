package eventprojection

import (
	"context"
)

// Consumer is an interface for used to open cursors for reading streams of
// events.
type Consumer interface {
	// OpenAt returns a cursor used to consume events beginning at a specific
	// offset.
	OpenAt(ctx context.Context, streamID string, offset uint64) (Cursor, error)
}

// Cursor is an interface for reading events from a stream in order.
type Cursor interface {
	// Next returns the next event in the stream.
	//
	// If the end of the stream is reached it blocks until a relevant event is
	// appended to the stream, ctx is canceled or the stream is closed.
	//
	// If ok is false the stream has been closed and no more events are expected.
	Next(ctx context.Context) (_ Event, ok bool, _ error)

	// Close closes the cursor.
	//
	// It must be called whenever the cursor is no longer needed.
	Close() error
}
