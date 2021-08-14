package streamprojection

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
	"go.etcd.io/bbolt"
)

func NewBolt(
	db *bbolt.DB,
	h MessageHandler,
) (dogma.ProjectionMessageHandler, *Consumer) {
	if db == nil {
		return unboundhandler.New(h), &Consumer{}
	}

	key := identity.Key(h)

	repo := &boltRepository{
		ResourceRepository: boltprojection.NewResourceRepository(
			db,
			key,
		),
		db:  db,
		key: key,
	}

	return New(repo, h)
}

type boltRepository struct {
	*boltprojection.ResourceRepository

	db  *bbolt.DB
	key string
}

var (
	// rootBucket is the bucket at the root level that contains all data related
	// to stream projections.
	rootBucket = []byte("projection_stream")

	// snapshotsBucket is the bucket that contains stream snapshots.
	snapshotsBucket = []byte("snapshots")
)

func (rr *boltRepository) Load(ctx context.Context, id string) (StreamState, error) {
	ss := StreamState{
		StreamID: id,
	}

	return ss, rr.db.View(func(tx *bbolt.Tx) error {
		root := tx.Bucket(rootBucket)
		if root == nil {
			return nil
		}

		snapshots := root.Bucket(snapshotsBucket)
		if snapshots == nil {
			return nil
		}

		data := snapshots.Get([]byte(id))
		if data != nil {
			return json.Unmarshal(data, &ss)
		}

		return nil
	})
}

func (rr *boltRepository) Save(ctx context.Context, r, c, n []byte, updates []StreamState) (bool, error) {
	return false, errors.New("not implemented")
}

var _ Repository = (*boltRepository)(nil)
