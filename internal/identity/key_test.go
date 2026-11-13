package identity_test

import (
	"testing"
	"uuid"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/internal/identity"
)

func TestKey(t *testing.T) {
	id := uuid.New()

	h := &ProjectionMessageHandlerStub{
		ConfigureFunc: func(c dogma.ProjectionConfigurer) {
			c.Identity("<name>", id.String())
			c.Routes(
				dogma.HandlesEvent[*EventStub[TypeA]](),
			)
		},
	}

	got := Key(h)

	if got != id {
		t.Fatalf("unexpected identity: got %v, want %v", got, id)
	}
}
