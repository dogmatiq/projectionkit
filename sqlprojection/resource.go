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
	key string
	cs  candidateSet
}

func NewResourceRepository(
	db *sql.DB,
	key string,
	options ...Option,
) *ResourceRepository {
	rr := &ResourceRepository{
		db:  db,
		key: key,
	}

	rr.cs.init(db, options)

	return rr
}

var _ resource.Repository = (*ResourceRepository)(nil)

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	var v []byte

	return v, rr.withDriver(ctx, func(d Driver) error {
		var err error
		v, err = d.QueryVersion(ctx, rr.db, rr.key, r)
		return err
	})
}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	return rr.withDriver(ctx, func(d Driver) error {
		if len(v) == 0 {
			return d.DeleteResource(ctx, rr.db, rr.key, r)
		}

		return d.StoreVersion(ctx, rr.db, rr.key, r, v)
	})
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {
	return rr.withTx(ctx, func(d Driver, tx *sql.Tx) (bool, error) {
		return d.UpdateVersion(ctx, tx, rr.key, r, c, n)
	})
}

// UpdateResourceVersion updates the version of the resource r to n and performs
// a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, *sql.Tx) error,
) (ok bool, err error) {
	return rr.withTx(ctx, func(d Driver, tx *sql.Tx) (bool, error) {
		ok, err := d.UpdateVersion(ctx, tx, rr.key, r, c, n)
		if !ok || err != nil {
			return false, err
		}

		return true, fn(ctx, tx)
	})
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {
	return rr.withDriver(ctx, func(d Driver) error {
		return d.DeleteResource(ctx, rr.db, rr.key, r)
	})
}

// withDriver calls fn with the driver that should be used to perform SQL
// operations of rr.db.
func (rr *ResourceRepository) withDriver(
	ctx context.Context,
	fn func(Driver) error,
) error {
	d, err := rr.cs.resolve(ctx)
	if err != nil {
		return err
	}

	return fn(d)
}

// withTx calls fn with the driver that should be used to perform SQL operations
// of rr.db.
//
// fn is called within a transaction. The transaction is committed if fn returns
// nil; otherwise, it is rolled back.
func (rr *ResourceRepository) withTx(
	ctx context.Context,
	fn func(Driver, *sql.Tx) (bool, error),
) (bool, error) {
	var ok bool

	err := rr.withDriver(
		ctx,
		func(d Driver) error {
			tx, err := rr.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback() // nolint:errcheck

			ok, err = fn(d, tx)
			if err != nil {
				return err
			}

			if ok {
				return tx.Commit()
			}

			return tx.Rollback()
		},
	)

	return ok && err == nil, err
}
