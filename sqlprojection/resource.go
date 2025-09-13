package sqlprojection

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/projectionkit/resource"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in an SQL database.
type ResourceRepository struct {
	db  *sql.DB
	d   Driver
	key string
}

// NewResourceRepository returns a new [ResourceRepository] that uses db to
// store resource versions.
func NewResourceRepository(
	db *sql.DB,
	d Driver,
	key string,
) *ResourceRepository {
	return &ResourceRepository{
		db:  db,
		d:   d,
		key: key,
	}
}

var _ resource.Repository = (*ResourceRepository)(nil)

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return rr.d.QueryVersion(ctx, rr.db, rr.key, r)
}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	if len(v) == 0 {
		return rr.d.DeleteResource(ctx, rr.db, rr.key, r)
	}

	return rr.d.StoreVersion(ctx, rr.db, rr.key, r, v)
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {
	return rr.withTx(ctx, func(tx *sql.Tx) (bool, error) {
		return rr.d.UpdateVersion(ctx, tx, rr.key, r, c, n)
	})
}

// UpdateResourceVersionFn updates the version of the resource r to n and
// performs a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, *sql.Tx) error,
) (ok bool, err error) {
	return rr.withTx(ctx, func(tx *sql.Tx) (bool, error) {
		ok, err := rr.d.UpdateVersion(ctx, tx, rr.key, r, c, n)
		if !ok || err != nil {
			return false, err
		}

		return true, fn(ctx, tx)
	})
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {
	return rr.d.DeleteResource(ctx, rr.db, rr.key, r)
}

// withTx calls fn with the driver that should be used to perform SQL operations
// of rr.db.
//
// fn is called within a transaction. The transaction is committed if fn returns
// nil; otherwise, it is rolled back.
func (rr *ResourceRepository) withTx(
	ctx context.Context,
	fn func(*sql.Tx) (bool, error),
) (ok bool, err error) {
	tx, err := rr.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() // nolint:errcheck

	ok, err = fn(tx)
	if err != nil {
		return false, err
	}

	if ok {
		return true, tx.Commit()
	}

	return false, tx.Rollback()
}
