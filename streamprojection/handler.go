package streamprojection

import (
	"encoding"

	"github.com/dogmatiq/dogma"
)

type Snapshot interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	ApplyEvent(ev Event)
}

// Event is an interface for an event that is written to a stream.
type Event interface {
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

	// New returns a new empty snapshot.
	//
	// Repeated calls SHOULD return a value that is of the same type and
	// initialized in the same way. The return value MUST NOT be nil.
	New() Snapshot

	// RouteEventToStreams returns the IDs of the streams that will have events
	// recorded as a resolve of the event m.
	//
	// The IDs MUST be non-empty strings.
	//
	// The engine MUST NOT call RouteEventToStream() with any message of a
	// type that has not been configured for consumption by a prior call to
	// Configure(). If any such message is passed, the implementation MUST panic
	// with the UnexpectedMessage value.
	RouteEventToStreams(m dogma.Message) []string

	// HandleEvent transforms a Dogma event message into zero or more stream
	// event messages.
	HandleEvent(sn Snapshot, s EventScope, m dogma.Message)
}
