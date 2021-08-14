package eventprojection

import (
	"github.com/dogmatiq/dogma"
)

// Event is an interface for an event that is written to a stream.
type Event interface {
	MarshalBinary() ([]byte, error)
}

type MessageHandler interface {
	// Configure produces a configuration for this handler by calling methods on
	// the configurer c.
	//
	// The implementation MUST allow for multiple calls to Configure(). Each
	// call SHOULD produce the same configuration.
	//
	// The engine MUST call Configure() before calling HandleCommand(). It is
	// RECOMMENDED that the engine only call Configure() once per handler.
	Configure(c dogma.ProjectionConfigurer)

	// HandleEvent transforms a Dogma event message into zero or more stream
	// event messages.
	HandleEvent(s EventScope, m dogma.Message)
}
