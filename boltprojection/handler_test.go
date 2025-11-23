package boltprojection_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/projectionkit/boltprojection"
)

func TestNoCompactBehavior(t *testing.T) {
	var v NoCompactBehavior

	err := v.Compact(t.Context(), nil, nil)
	if err != nil {
		t.Fatal("unexpected error returned")
	}
}

func TestNoResetBehavior(t *testing.T) {
	var v NoResetBehavior

	err := v.Reset(t.Context(), nil, nil)
	if err != dogma.ErrNotSupported {
		t.Fatalf("unexpected error: got %v, want %v", err, dogma.ErrNotSupported)
	}
}
