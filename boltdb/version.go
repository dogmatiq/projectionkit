package boltdb

import (
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
	b, err := bucket(tx, topBucket, h)
	if err != nil {
		return false, err
	}
	// If the "current" version is empty, we assumed it's correct and that there
	// is no existing entry for this resource.
	if len(c) == 0 {
		// If the resource record already exists, that means the current version
		// was not correct.
		if v := b.Get(r); v == nil {
			return false, nil
		}
		if err = b.Put(r, n); err != nil {
			return false, err
		}
	}

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the bucket KV entry
		// entirely.
		if err = b.Delete(r); err != nil {
			return false, err
		}
	} else {
		if err = b.Put(r, n); err != nil {
			return false, err
		}
	}

	return true, nil
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
	b, err := bucket(tx, topBucket, h)
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

	b, err := bucket(tx, topBucket, h)
	if err != nil {
		return err
	}

	if err = b.Delete(r); err != nil {
		return err
	}
	return tx.Commit()
}
