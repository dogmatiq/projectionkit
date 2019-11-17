package boltdb

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

const (
	// TopBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	TopBucket = "projection_occ"
)

// bucket retrieves the deepest bucket at a given bucket hierarchy. Slice bb
// represents the bucket hierarchy with the first element being the top level
// bucket and the last being the bucket nested at len(bb) level.
//
// If tx is writable this function attempts to create any missing buckets in bb.
// Otherwise, this function produces an error notifying about the first missing
// bucket encountered in bb.
func bucket(tx *bolt.Tx, bb ...string) (*bolt.Bucket, error) {
	var (
		bkt *bolt.Bucket
		err error
	)

	for _, b := range bb {
		if bkt == nil {
			if tx.Writable() {
				if bkt, err = tx.CreateBucketIfNotExists(
					[]byte(b),
				); err != nil {
					return nil, err
				}
			} else {
				if bkt = tx.Bucket([]byte(b)); bkt == nil {
					return nil, fmt.Errorf("bucket '%s' not found", b)
				}
			}
			continue
		}

		if tx.Writable() {
			if bkt, err = bkt.CreateBucketIfNotExists(
				[]byte(b),
			); err != nil {
				return nil, err
			}
		} else {
			if bkt = bkt.Bucket([]byte(b)); bkt == nil {
				return nil, fmt.Errorf("bucket '%s' not found", b)
			}
		}
	}

	return bkt, nil
}
