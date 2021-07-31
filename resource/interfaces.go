package resource

import (
	"context"
)

// storer is an interface for projection message handlers that support
// unconditionally storing resource versions.
//
// A type that implements this interface can still indicate a lack of support
// for its operations by returning ErrNotSupported.
type storer interface {
	StoreResourceVersion(ctx context.Context, r, v []byte) error
}

// updater is an interface for projection message handlers that support updating
// resource versions without handling an event.
//
// A type that implements this interface can still indicate a lack of support
// for its operations by returning ErrNotSupported.
type updater interface {
	UpdateResourceVersion(ctx context.Context, r, c, n []byte) (bool, error)
}

// deleter is an interface for projection message handlers that support deleting
// all notions of a resource from their data store.
//
// A type that implements this interface can still indicate a lack of support
// for its operations by returning ErrNotSupported.
type deleter interface {
	DeleteResource(ctx context.Context, r []byte) error
}
