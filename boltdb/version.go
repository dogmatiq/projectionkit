package boltdb

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

// updateVersion updates a resource's version within a BoltDB transaction using
// a handler key, resource name, current and next resource version.
//
// This function discards a resource version record in the database if the
// next version is empty.
//
// This function returns an error if a current resource version does not match
// the version value persisted in the database.
func updateVersion(
	ctx context.Context,
	tx *bolt.Tx,
	hk string,
	r, c, n []byte,
) (bool, error) {
	// Retrieve/create a handler bucket.
	b, err := makeHandlerBucket(tx, hk)
	if err != nil {
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

// queryVersion queries the resource version from the database with a given
// handler key and resource name.
//
// If there is no version persisted for a given resource, a nil is returned.
func queryVersion(
	ctx context.Context,
	db *bolt.DB,
	hk string,
	r []byte,
) ([]byte, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b := handlerBucket(tx, hk); b != nil {
		return b.Get(r), nil
	}

	return nil, nil
}

// deleteResource discards a resource version record from the database using a
// handler key and resource name.
func deleteResource(
	ctx context.Context,
	db *bolt.DB,
	hk string,
	r []byte,
) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if b := handlerBucket(tx, hk); b != nil {
		if err = b.Delete(r); err != nil {
			return err
		}
	}

	return tx.Commit()
}
