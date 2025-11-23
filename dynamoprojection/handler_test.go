package dynamoprojection_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
)

func TestNoCompactBehavior(t *testing.T) {
	var v NoCompactBehavior

	if err := v.Compact(
		t.Context(),
		nil, // client
		&ProjectionCompactScopeStub{},
	); err != nil {
		t.Fatal(err)
	}
}

func TestNoResetBehavior(t *testing.T) {
	var v NoResetBehavior

	if _, err := v.Reset(
		t.Context(),
		&ProjectionResetScopeStub{},
	); err != dogma.ErrNotSupported {
		t.Fatalf("unexpected error: got %v, want %v", err, dogma.ErrNotSupported)
	}
}
