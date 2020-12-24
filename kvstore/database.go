package kvstore

import "context"

// Store is an interface for a transactional key/value store.
//
// Each database contains a set of "root" keyspaces, each of which can contain
// key/value pairs as well as "child" keyspaces. Each keyspace is identified by
// a name.
//
// Keyspace lifetime is managed automatically. A keyspace exists if it contains
// at least one key. Removing the last key from a keyspace causes the keyspace
// to be deleted.
//
// Keyspace names, keys and values are all opaque binary data represented as
// byte slices. nil and empty byte slices are considered equivalent. An
// implementation may use them interchangably, so consumers should always use
// len(slice) to check for emptiness, and never compare to nil.
//
// With a keyspace, the storage of child keyspaces and key/value pairs are
// separate. A keyspace name can not conflict with a key.
//
// Locking and concurrency semantics are implementation defined.
type Store interface {
	// RBegin starts a new read-only transaction.
	//
	// The caller must close the transaction when it is no longer needed.
	//
	// All operations performed within the transaction are subject to the
	// liveness of ctx. If ctx is canceled the transaction is no longer usable.
	//
	// Neither the transaction nor the keyspaces obtained from it are safe for
	// concurrent use by multiple goroutines.
	RBegin(ctx context.Context) (RTx, error)

	// Begin starts a new read/write transaction.
	//
	// The caller must close the transaction when it is no longer needed, even
	// if it has been successfully (or unsuccessfully) committed.
	//
	// All operations performed within the transaction are subject to the
	// liveness of ctx. If ctx is canceled the transaction is no longer usable.
	//
	// Neither the transaction nor the keyspaces obtained from it are safe for
	// concurrent use by multiple goroutines.
	Begin(ctx context.Context) (Tx, error)
}

// RTx is a read-only transaction.
//
// It is a subset of the Tx interface.
//
// It may not be used by multiple goroutines concurrently.
type RTx interface {
	// RSpace returns a read-only reference to a root keyspace.
	RSpace(name []byte) RSpace

	// Close ends the transaction.
	//
	// Close must be called when the transaction is no longer needed.
	Close()
}

// Tx is a read/write transaction.
//
// It is a superset of the RTx interface.
//
// It may not be used by multiple goroutines concurrently.
type Tx interface {
	RTx

	// Space returns a reference to a root keyspace.
	Space(name []byte) Space

	// Commit applies the changes made within the transaction.
	//
	// It does not close the transaction, the caller must still call Close()
	// when the transaction is no longer needed.
	Commit() error
}

// RSpace is an interface for reading values within a single keyspace.
//
// It is a subset of the Space interface.
//
// The keyspace is tied to the transaction that created it and is not safe for
// concurrent use.
//
// Any byte-slices obtained from the RSpace are only valid for the lifetime of
// the transaction. They must be copied if they are to be used after the
// transaction is closed.
type RSpace interface {
	// Name returns the name of the keyspace.
	Name() string

	// RSpace returns a read-only reference to a child keyspace.
	RSpace(name []byte) RSpace

	// Get returns the value associated with a specific key.
	Get(key []byte) (value []byte, exists bool, err error)

	// Has returns true if the given key exists.
	Has(key []byte) (bool, error)

	// Range calls fn for each key/value pair in the keyspace.
	//
	// err is non-nil if fn returns an error or there is some problem loading
	// data from the store.
	//
	// exhausted is true if fn sets cont to true for every key/value pair.
	// That is, if cont is false iteration stops and exhausted is false.
	//
	// The order of iteration is undefined.
	Range(
		fn func(key, value []byte) (cont bool, _ error),
	) (exhausted bool, err error)

	// RangeRSpaces calls fn for each child keyspace.
	//
	// err is non-nil if fn returns an error or there is some problem loading
	// data from the store.
	//
	// exhausted is true if fn sets cont to true for every key/value pair.
	// That is, if cont is false iteration stops and exhausted is false.
	//
	// The order of iteration is undefined.
	RangeRSpaces(
		fn func(RSpace) (cont bool, _ error),
	) (exhausted bool, err error)
}

// Iter is a function for ranging over the keys and values within a keyspace.
type Iter func(k, v []byte) (bool, error)

// Space is an interface for reading and writing values within a keyspace.
//
// It is a superset of the RSpace interface.
//
// The keyspace is tied to the transaction that created it and is not safe for
// concurrent use.
type Space interface {
	RSpace

	// Space returns a reference to a child keyspace.
	Space(name []byte) Space

	// Set associates a value with a key within the keyspace.
	Set(key, value []byte) error

	// Delete deletes a single key within the keyspace.
	//
	// It is not an error to delete a non-existent key.
	//
	// If this key is the last key within the keyspace, the keyspace itself is
	// deleted.
	Delete(key []byte) error

	// DeleteAll deletes all keys within this keyspace, thus deleting the
	// keyspace itself.
	DeleteAll() error

	// RangeSpaces calls fn for each child keyspace.
	//
	// err is non-nil if fn returns an error or there is some problem loading
	// data from the store.
	//
	// exhausted is true if fn sets cont to true for every key/value pair.
	// That is, if cont is false iteration stops and exhausted is false.
	//
	// The order of iteration is undefined.
	RangeSpaces(
		fn func(Space) (cont bool, _ error),
	) (exhausted bool, err error)
}
