package boltdb

import (
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const (
	// topBucket is the bucket at the root level that contains all data related
	// to projection OCC.
	topBucket = "projection_occ"
)

// mkBucketAll creates bucket hierarchy and returns the deepest bucket in it.
// Slice bb represents the bucket hierarchy with the first element being the top
// level bucket and the last being the bucket nested at len(bb) level.
//
// In order to successfully create a bucket hierarchy, tx must be writable or an
// error is returned. If some or all buckets in bb already exist, this function
// ignores creation.
func mkBucketAll(tx *bolt.Tx, bb ...string) (*bolt.Bucket, error) {
	// return if no buckets passed
	if len(bb) == 0 {
		return nil, nil
	}

	type bktcreator interface {
		CreateBucketIfNotExists(name []byte) (*bolt.Bucket, error)
	}

	var (
		bc  bktcreator = tx
		err error
	)

	for _, b := range bb {
		if bc, err = bc.CreateBucketIfNotExists(
			[]byte(b),
		); err != nil {
			return nil, err
		}
	}

	return bc.(*bolt.Bucket), nil
}

// bucket retrieves the deepest bucket at a given bucket hierarchy. Slice bb
// represents the bucket hierarchy with the first element being the top level
// bucket and the last being the bucket nested at len(bb) level.
//
// if any of the buckets in bb don't exist, this function produces an error
// notifying about the first missing bucket encountered in bb.
func bucket(tx *bolt.Tx, bb ...string) (*bolt.Bucket, error) {
	// return if no buckets passed
	if len(bb) == 0 {
		return nil, nil
	}

	type bktgetter interface {
		Bucket(name []byte) *bolt.Bucket
	}

	var bg bktgetter = tx

	for i, name := range bb {
		if bg = bg.Bucket([]byte(name)); bg == nil {
			return nil,
				fmt.Errorf(
					"bucket '%s' not found",
					strings.Join(bb[i+1:], "."),
				)
		}
	}
	return bg.(*bolt.Bucket), nil
}
