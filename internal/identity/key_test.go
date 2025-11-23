package identity_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/projectionkit/internal/identity"
)

func TestKey(t *testing.T) {
	id := uuidpb.Generate()

	h := &ProjectionMessageHandlerStub{
		ConfigureFunc: func(c dogma.ProjectionConfigurer) {
			c.Identity("<name>", id.AsString())
			c.Routes(
				dogma.HandlesEvent[*EventStub[TypeA]](),
			)
		},
	}

	got := Key(h)
	want := uuidpb.AsByteArray[[16]byte](id)

	if got != want {
		t.Fatalf("unexpected identity: got %v, want %v", got, want)
	}
}
