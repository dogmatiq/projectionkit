package boltdb

import (
	bolt "go.etcd.io/bbolt"
)

const (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = "projection_occ"
)

// makeHandlerBucket creates a bucket for a given handler key if it has not been
// created yet.
//
// This function returns an error it tx is not writable. This function panics if
// the passed handler's key is an empty string.
func makeHandlerBucket(tx *bolt.Tx, hk string) (*bolt.Bucket, error) {
	tb, err := tx.CreateBucketIfNotExists([]byte(topBucket))
	if err != nil {
		return nil, err
	}

	hb, err := tb.CreateBucketIfNotExists([]byte(hk))
	if err != nil {
		return nil, err
	}

	return hb, nil
}

// handlerBucket retrieves a bucket for a given handler key. If a bucket with
// a given handler key does not exist, this function returns nil.
func handlerBucket(tx *bolt.Tx, hk string) *bolt.Bucket {
	tb := tx.Bucket([]byte(topBucket))
	if tb == nil {
		return nil
	}

	return tb.Bucket([]byte(hk))
}
