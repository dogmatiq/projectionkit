package eventprojection

import (
	"context"

	"github.com/dogmatiq/projectionkit/resource"
)

// A Repository stores the projection's resource versions and stream state.
type Repository interface {
	resource.Repository

	// UpdateStreams appends a set of events to a stream.
	//
	// It updates the version of the resource r to n. If c is not the current
	// version of r, it returns false and no update occurs.
	UpdateStreams(
		ctx context.Context,
		r, c, n []byte,
		changes map[string]*StreamChanges,
	) (bool, error)
}
