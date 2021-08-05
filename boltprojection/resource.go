package boltprojection

import (
	"bytes"
	"context"

	"github.com/dogmatiq/projectionkit/resource"
	"go.etcd.io/bbolt"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in a BoltDB database.
type ResourceRepository struct {
	db  *bbolt.DB
	key string
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new BoltDB resource repository.
func NewResourceRepository(
	db *bbolt.DB,
	key string,
) *ResourceRepository {
	return &ResourceRepository{db, key}
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	var v []byte

	return v, rr.db.View(func(tx *bbolt.Tx) error {
		if b := handlerBucket(tx, rr.key); b != nil {
			v = b.Get(r)
		}

		return nil
	})
}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	return rr.db.Update(func(tx *bbolt.Tx) error {
		b, err := makeHandlerBucket(tx, rr.key)
		if err != nil {
			// CODE COVERAGE: This branch can not be easily covered without somehow
			// breaking the BoltDB connection or the database file in some way.
			return err
		}

		if len(v) == 0 {
			// If the version is empty, we can delete the bucket KV entry.
			return b.Delete(r)
		}

		// We can finally update the version.
		return b.Put(r, v)
	})
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {
	return ok, rr.db.Update(func(tx *bbolt.Tx) error {
		var err error
		ok, err = rr.updateResourceVersion(tx, r, c, n)
		return err
	})
}

// UpdateResourceVersion updates the version of the resource r to n and performs
// a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, *bbolt.Tx) error,
) (ok bool, err error) {
	return ok, rr.db.Update(func(tx *bbolt.Tx) error {
		var err error
		ok, err = rr.updateResourceVersion(tx, r, c, n)
		if !ok || err != nil {
			return err
		}

		return fn(ctx, tx)
	})
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) updateResourceVersion(
	tx *bbolt.Tx,
	r, c, n []byte,
) (bool, error) {
	b, err := makeHandlerBucket(tx, rr.key)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return false, err
	}

	// If the "current" version is different to the value associated with
	// the resource's key, that means the current version was not correct.
	if !bytes.Equal(b.Get(r), c) {
		return false, nil
	}

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the bucket KV entry.
		return true, b.Delete(r)
	}

	// We can finally update the next version.
	return true, b.Put(r, n)
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {
	return rr.db.Update(func(tx *bbolt.Tx) error {
		if b := handlerBucket(tx, rr.key); b != nil {
			return b.Delete(r)
		}

		return nil
	})
}

var (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = []byte("projection_occ")
)

// makeHandlerBucket creates a bucket for the given handler key if it has not
// been created yet.
//
// This function returns an error it tx is not writable.
func makeHandlerBucket(tx *bbolt.Tx, hk string) (*bbolt.Bucket, error) {
	tb, err := tx.CreateBucketIfNotExists(topBucket)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return nil, err
	}

	return tb.CreateBucketIfNotExists([]byte(hk))
}

// handlerBucket retrieves a bucket for the given handler key. If a bucket with
// the given handler key does not exist, this function returns nil.
func handlerBucket(tx *bbolt.Tx, hk string) *bbolt.Bucket {
	tb := tx.Bucket(topBucket)
	if tb == nil {
		return nil
	}

	return tb.Bucket([]byte(hk))
}
