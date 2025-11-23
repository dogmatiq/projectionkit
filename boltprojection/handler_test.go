package boltprojection_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/boltprojection"
)

func TestNoCompactBehavior(t *testing.T) {
	var v NoCompactBehavior

	if err := v.Compact(
		t.Context(),
		nil, // db
		&ProjectionCompactScopeStub{},
	); err != nil {
		t.Fatal("unexpected error returned")
	}
}

func TestNoResetBehavior(t *testing.T) {
	var v NoResetBehavior

	if err := v.Reset(
		t.Context(),
		nil, // tx
		&ProjectionResetScopeStub{},
	); err != dogma.ErrNotSupported {
		t.Fatalf("unexpected error: got %v, want %v", err, dogma.ErrNotSupported)
	}
}
