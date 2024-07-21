package observableprojection

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
)

type Adaptor[
	S State[D],
	D Delta[S],
] struct {
	Handler MessageHandler[S, D]
}

var _ dogma.ProjectionMessageHandler = &Adaptor[int, int]{}

func (a *Adaptor[S, D]) Configure(c dogma.ProjectionConfigurer) {
	if a.Handler != nil {
		a.Handler.Configure(c)
	} else {
		c.Disable()
	}
}

func (a *Adaptor[S, D]) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	e dogma.Event,
) (bool, error) {
}

// ResourceVersion returns the current version of a resource.
//
// It returns an empty slice if r is not in the OCC store.
func (a *Adaptor[S, D]) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
}

// CloseResource informs the handler that the engine has no further use for
// a resource.
//
// If r is present in the OCC store the handler SHOULD remove it.
func (a *Adaptor[S, D]) CloseResource(ctx context.Context, r []byte) error {
}

// TimeoutHint returns a suitable duration for handling the given event.
//
// The duration SHOULD be as short as possible. If no hint is available it
// MUST be zero.
//
// In this context, "timeout" refers to a deadline, not a timeout message.
func (a *Adaptor[S, D]) TimeoutHint(Message) time.Duration {
	return 0
}

// Compact attempts to reduce the size of the projection.
//
// For example, it may delete unused data, or merge overly granular data.
//
// The handler SHOULD compact the projection incrementally such that it
// makes some progress even if the context's deadline expires.
func (a *Adaptor[S, D]) Compact(context.Context, ProjectionCompactScope) error {
}
