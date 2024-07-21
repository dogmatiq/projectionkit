package observableprojection

import (
	"github.com/dogmatiq/dogma"
)

type State[D any] interface{}

type Delta[S any] interface {
	Apply(S) S
}

// MessageHandler is a specialization of [dogma.ProjectionMessageHandler] that
// builds an in-memory projection represented by a value of type T.
type MessageHandler[
	S State[D],
	D Delta[S],
] interface {
	// Configure produces a configuration for this handler by calling methods on
	// the configurer c.
	//
	// The implementation MUST allow for multiple calls to Configure(). Each
	// call SHOULD produce the same configuration.
	//
	// The engine MUST call Configure() before calling HandleEvent(). It is
	// RECOMMENDED that the engine only call Configure() once per handler.
	Configure(c dogma.ProjectionConfigurer)

	RouteEventToInstances(m dogma.Message) []string

	HandleEvent(
		instanceID string,
		state S,
		s dogma.ProjectionEventScope,
		m dogma.Message,
	) []D
}
