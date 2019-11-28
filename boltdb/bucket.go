package boltdb

import (
	bolt "go.etcd.io/bbolt"
)

const (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = "projection_occ"
)

// makeHandlerBucket creates a bucket for the given handler key if it has not
// been created yet.
//
// This function returns an error it tx is not writable.
func makeHandlerBucket(tx *bolt.Tx, hk string) (*bolt.Bucket, error) {
	tb, err := tx.CreateBucketIfNotExists([]byte(topBucket))
	if err != nil {
		return nil, err
	}

	return tb.CreateBucketIfNotExists([]byte(hk))
}

// handlerBucket retrieves a bucket for the given handler key. If a bucket with
// the given handler key does not exist, this function returns nil.
func handlerBucket(tx *bolt.Tx, hk string) *bolt.Bucket {
	tb := tx.Bucket([]byte(topBucket))
	if tb == nil {
		return nil
	}

	return tb.Bucket([]byte(hk))
}
