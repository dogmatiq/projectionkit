package boltprojection_test

import (
	"context"
	"testing"

	. "github.com/deslittle/projectionkit/boltprojection"
)

func TestNoCompactBehavior_Compact_ReturnsNil(t *testing.T) {
	var v NoCompactBehavior

	err := v.Compact(context.Background(), nil, nil)

	if err != nil {
		t.Fatal("unexpected error returned")
	}
}
