package resource

import "context"

// storer is an interface for projection message handlers that support
// unconditionally storing resource versions.
type storer interface {
	StoreResourceVersion(ctx context.Context, r, v []byte) error
}

// updater is an interface for projection message handlers that support updating
// resource versions without handling an event.
type updater interface {
	UpdateResourceVersion(ctx context.Context, r, c, n []byte) (bool, error)
}

// deleter is an interface for projection message handlers that support deleting
// all notions of a resource from their data store.
type deleter interface {
	DeleteResource(ctx context.Context, r []byte) error
}
