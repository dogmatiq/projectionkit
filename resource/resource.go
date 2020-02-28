package resource

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
)

// ErrNotSupported indicates that the handler does not support a low-level
// resource operation.
var ErrNotSupported = errors.New("the handler does not support this operation")

// StoreVersion unconditionally sets the version of the resource r to v.
//
// Care should be taken using this function, as it bypasses the
// optimistic-concurrency checks normally present when updating a resource
// version. It's almost always better to use UpdateVersion() instead.
//
// It returns ErrNotSupported if the handler does not support storing resource
// versions outside of when an event is handled.
func StoreVersion(
	ctx context.Context,
	h dogma.ProjectionMessageHandler,
	r, v []byte,
) error {
	// If the handler directly supports storing versions, use that.
	if s, ok := h.(storer); ok {
		return s.StoreResourceVersion(ctx, r, v)
	}

	// Otherwise, load the resource version and attempt to update it in a loop.
	u, ok := h.(updater)
	if !ok {
		return ErrNotSupported
	}

	for {
		c, err := h.ResourceVersion(ctx, r)
		if err != nil {
			return err
		}

		ok, err := u.UpdateResourceVersion(ctx, r, c, v)
		if ok || err != nil {
			return err
		}
	}
}

// UpdateVersion updates the version of the resource r to n if the current
// version is c.
//
// It returns false if c is not the current version.
//
// It returns ErrNotSupported if the handler does not support storing resource
// versions outside of when an event is handled.
func UpdateVersion(
	ctx context.Context,
	h dogma.ProjectionMessageHandler,
	r, c, n []byte,
) (ok bool, err error) {
	u, ok := h.(updater)
	if !ok {
		return false, ErrNotSupported
	}

	return u.UpdateResourceVersion(ctx, r, c, n)
}

// DeleteResource removes all information about the resource r from the
// handler's data store.
//
// For some handler implementations, this is *likely* to be an equivalent
// operation to h.CloseResource(), however handler implementations are not
// *required* to remove data about a resource when it is closed.
//
// Furthermore, the result of deleting a resource should be that the handler
// will behave as though it had never had such a resource, whereas the behavior
// of a projection when using a closed resource is undefined.
//
// It returns ErrNotSupported if the handler does not support deleting
// resources.
func DeleteResource(
	ctx context.Context,
	h dogma.ProjectionMessageHandler,
	r []byte,
) (bool error) {
	d, ok := h.(deleter)
	if !ok {
		return ErrNotSupported
	}

	return d.DeleteResource(ctx, r)
}
