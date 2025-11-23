package memoryprojection_test

import (
	"testing"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/memoryprojection"
)

func TestNoCompactBehavior(t *testing.T) {
	var v NoCompactBehavior[int]

	if value := v.Compact(
		123,
		&ProjectionCompactScopeStub{},
	); value != 123 {
		t.Fatalf("unexpected value: got %v, want %v", value, 123)
	}
}
