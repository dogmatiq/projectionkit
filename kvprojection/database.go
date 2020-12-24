package kvprojection

import "context"

// DB is an interface for a key value store.
type DB interface {
	// Begin starts a new transaction.
	Begin(ctx context.Context) (Tx, error)
}

// Tx is a transaction for manipulating the key/value store.
type Tx interface {
	Get(ctx context.Context, keyspace string, k []byte) ([]byte, error)

	Set(ctx context.Context, keyspace string, k, v []byte) error

	Delete(ctx context.Context, keyspace string, k []byte) error

	Range(
		ctx context.Context,
		keyspace string,
		fn Iter,
	) bool

	Commit() error

	Rollback() error
}

// Iter is a function for ranging over the keys and values within a keyspace.
type Iter func(k string, v []byte) bool
