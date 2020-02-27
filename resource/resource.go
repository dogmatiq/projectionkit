package resource

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
)

// ErrNotSupported indicates that the handler does not a low-level operation.
var ErrNotSupported = errors.New("the handler does not support this operation")

// StoreVersion sets the version of the resource r to v.
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
