package kvprojection

import (
	"bytes"
	"context"
)

type RangeFunc func(ctx context.Context, key, value []byte) error

type KeyValueStore interface {
	Load(ctx context.Context, key []byte) (value []byte, err error)
	Save(ctx context.Context, changeset ...Change) (bool, error)
	Range(ctx context.Context, fn RangeFunc) error
}

type Change struct {
	Key         []byte
	ValueBefore []byte
	ValueAfter  []byte
}

var resourceKeyPrefix = []byte("_occ:")

func resourceKey(hk, r []byte) []byte {
	var w bytes.Buffer

	w.Write(resourceKeyPrefix)
	w.Write(hk)
	w.Write(r)

	return w.Bytes()
}

func isResourceKey(k []byte) bool {
	return bytes.HasPrefix(k, resourceKeyPrefix)
}
