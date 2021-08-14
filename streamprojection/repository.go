package streamprojection

import (
	"context"
	"time"

	"github.com/dogmatiq/projectionkit/resource"
)

// A Repository stores the projection's resource versions and stream state.
type Repository interface {
	resource.Repository

	// Load returns the state of the stream with the given ID.
	//
	// If the stream does not exist it returns a StreamState at version zero.
	Load(ctx context.Context, id string) (StreamState, error)

	// Save saves the state of a set of streams.
	//
	// It updates the version of the resource r to n. If c is not the current
	// version of r, it returns false and no update occurs.
	//
	// If streams is empty it is equivalent to UpdateResourceVersion().
	Save(ctx context.Context, r, c, n []byte, streams []StreamState) (bool, error)
}

// StreamState is the state of a stream that is persisted using a Repository.
type StreamState struct {
	StreamID string
	Version  int64
	Snapshot []byte
	IsClosed bool
	ExpireAt time.Time
}
