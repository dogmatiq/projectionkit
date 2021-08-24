package eventprojection

import (
	"github.com/dogmatiq/dogma"
)

// Event is an interface for an event that is produced by a MessageHandler.
type Event interface {
}

// MessageHandler is a specialization of dogma.ProjectionMessageHandler for
// building projections that are themselves ordered streams of events.
//
// The handler consumes Dogma events (represented by the dogma.Message
// interface) just like any other projection, and produces "projected" events
// which are represented by the Event interface in this package.
//
// Each Dogma event can produce 0 or more projected events.
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

	// MarshalEvent marshals an event to its binary representation.
	MarshalEvent(ev Event) ([]byte, error)

	// UnmarshalEvent marshals an event to its binary representation.
	UnmarshalEvent([]byte) (Event, error)
}
