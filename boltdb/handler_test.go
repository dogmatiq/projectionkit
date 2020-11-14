package boltdb_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/projectionkit/boltdb"
)

func TestNoCompactBehaviorBehavior_Compact_ReturnsNil(t *testing.T) {
	var v NoCompactBehavior

	err := v.Compact(context.Background(), nil, nil)

	if err != nil {
		t.Fatal("unexpected error returned")
	}
}
