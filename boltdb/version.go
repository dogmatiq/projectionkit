package boltdb

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

// updateVersion updates a resource's version within a BoltDB transaction.
//
// It deletes the resource from the database if n is empty.
//
// It returns false if the current version c does not match the version in the
// database.
func updateVersion(
	ctx context.Context,
	tx *bolt.Tx,
	hk string,
	r, c, n []byte,
) (bool, error) {
	b, err := makeHandlerBucket(tx, hk)
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

// queryVersion returns the current version of a resource from the database.
//
// It returns nil if there is no version persisted for the resource.
func queryVersion(
	ctx context.Context,
	db *bolt.DB,
	hk string,
	r []byte,
) ([]byte, error) {
	tx, err := db.Begin(false)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return nil, err
	}
	defer tx.Rollback()

	if b := handlerBucket(tx, hk); b != nil {
		return b.Get(r), nil
	}

	return nil, nil
}

// deleteResource ensures that a resource does not exist in the database.
func deleteResource(
	ctx context.Context,
	db *bolt.DB,
	hk string,
	r []byte,
) error {
	tx, err := db.Begin(true)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the BoltDB connection or the database file in some way.
		return err
	}
	defer tx.Rollback()

	if b := handlerBucket(tx, hk); b != nil {
		if err := b.Delete(r); err != nil {
			// CODE COVERAGE: This branch can not be easily covered without
			// somehow breaking the BoltDB connection or the database file in
			// some way.
			return err
		}
	}

	return tx.Commit()
}
