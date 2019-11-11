package boltdb

import (
	bolt "go.etcd.io/bbolt"
)

const (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = "projection_occ"
)

// bucket retrieves the deepest bucket at a given bucket hierarchy. Slice bb
// represents the bucket hierarchy with the first element being the top level
// bucket and the last being the bucket nested at len(bb) level.
//
// If any bucket is missing at any level, this function creates it.
func bucket(tx *bolt.Tx, bb ...string) (*bolt.Bucket, error) {
	var (
		bkt *bolt.Bucket
		err error
	)
	for _, b := range bb {
		if bkt == nil {
			if bkt, err = tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return nil, err
			}
		} else {
			if bkt, err = bkt.CreateBucketIfNotExists([]byte(b)); err != nil {
				return nil, err
			}
		}

	}
	return bkt, nil
}
