package boltdb

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

func updateVersion(
	ctx context.Context,
	tx *bolt.Tx,
	h string,
	r, c, n []byte,
) (bool, error) {
	// Retrieve/create the handler bucket.
	b, err := bucket(tx, TopBucket, h)
	if err != nil {
		return false, err
	}

	// If the "current" version is not nil (i.e. the first call for the handler)
	// and different to the value in the resource bucket, that means the current
	// version was not correct.
	if c1 := b.Get(r); c1 != nil && !bytes.Equal(c, c1) {
		return false, nil
	}

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the bucket KV entry
		// entirely.
		return true, b.Delete(r)
	}

	// We can finally update the next version.
	return true, b.Put(r, n)
}

func queryVersion(
	ctx context.Context,
	db *bolt.DB,
	h string,
	r []byte,
) ([]byte, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b, err := bucket(tx, TopBucket, h)
	if err != nil {
		return nil, err
	}

	return b.Get(r), nil
}

func deleteResource(
	ctx context.Context,
	db *bolt.DB,
	h string,
	r []byte,
) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, err := bucket(tx, TopBucket, h)
	if err != nil {
		return err
	}

	if err = b.Delete(r); err != nil {
		return err
	}

	return tx.Commit()
}
