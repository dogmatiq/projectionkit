package neo4jprojection_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/projectionkit/neo4jprojection"
)

func TestNoCompactBehavior_Compact_ReturnsNil(t *testing.T) {
	var v NoCompactBehavior

	err := v.Compact(context.Background(), nil, nil)

	if err != nil {
		t.Fatal("unexpected error returned")
	}
}
