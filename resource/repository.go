package resource

import (
	"context"
)

// RepositoryAware is an interface for projection message handlers that can
// provide a resource repository for low-level manipulation of resource
// versions.
type RepositoryAware interface {
	// ResourceRepository returns a repository that can be used to manipulate
	// the handler's resource versions.
	ResourceRepository(context.Context) (Repository, error)
}

// Repository is an interface for low-level management of resource versions.
//
// This interface is intended to aid implementing higher-order projections on
// top of the projection types provided by this module.
//
// For general management of persisted resource versions the functions in this
// package should be used instead.
type Repository interface {
	// ResourceVersion returns the version of the resource r.
	ResourceVersion(ctx context.Context, r []byte) ([]byte, error)

	// StoreResourceVersion sets the version of the resource r to v without
	// checking the current version.
	StoreResourceVersion(ctx context.Context, r, v []byte) error

	// UpdateResourceVersion updates the version of the resource r to n.
	//
	// If c is not the current version of r, it returns false and no update occurs.
	UpdateResourceVersion(ctx context.Context, r, c, n []byte) (bool, error)

	// DeleteResource removes all information about the resource r.
	DeleteResource(ctx context.Context, r []byte) error
}
