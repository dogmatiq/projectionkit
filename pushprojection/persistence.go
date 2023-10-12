package pushprojection

import "context"

type Store interface {
	Save(
		ctx context.Context,
		hk, r, c, n []byte,
		partition, data []byte,
	) error

	Load(
		ctx context.Context,
		hk, partition []byte,
	) (data []byte, err error)
}
