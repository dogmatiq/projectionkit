package unboundhandler

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
)

// UpstreamHandler is a handler that adheres to one of the MessageHandler
// interfaces within projectionkit.
type UpstreamHandler interface {
	Configure(dogma.ProjectionConfigurer)
}

// errUnbound is returned by any projection operation that requires a database.
var errUnbound = errors.New("projection handler has not been bound to a database")

// handler is an implementation of [dogma.ProjectionMessageHandler] that
// represents a projectionkit handler that has not been bound to a database.
type handler struct {
	UpstreamHandler
}

// New adapts a projectionkit message handler that has not been bound to a
// specific database into a Dogma projection message handler.
//
// Any operations that require access to the database return an error.
func New(h UpstreamHandler) dogma.ProjectionMessageHandler {
	return handler{h}
}

func (h handler) HandleEvent(
	context.Context,
	dogma.ProjectionEventScope,
	dogma.Event,
) (uint64, error) {
	return 0, errUnbound
}

func (h handler) CheckpointOffset(context.Context, string) (uint64, error) {
	return 0, errUnbound

}

func (h handler) Compact(context.Context, dogma.ProjectionCompactScope) error {
	return errUnbound
}
