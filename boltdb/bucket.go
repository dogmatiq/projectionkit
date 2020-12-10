package boltdb

import "go.etcd.io/bbolt"

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
