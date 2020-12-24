package patchprojection

import (
	"github.com/dogmatiq/dogma"
)

// A MessageHandler is a specialization of dogma.ProjectionMessageHandler that
// produces State and Patch values from Dogma events.
//
// The HandleEvent(), MarshalState() and UnmarshalState() methods MUST NOT be
// called concurrently.
type MessageHandler interface {
	// Configure produces a configuration for this handler by calling methods on
	// the configurer c.
	//
	// The implementation MUST allow for multiple calls to Configure(). Each
	// call SHOULD produce the same configuration.
	//
	// The engine MUST call Configure() before calling HandleEvent(). It is
	// RECOMMENDED that the engine only call Configure() once per handler.
	Configure(c dogma.ProjectionConfigurer)

	// HandleEvent updates the state to reflect the occurence of a Dogma event.
	//
	// It returns a set of patches that can be applied to the previous state to
	// produce the new state.
	//
	// Any given event may result in any number of patches, including zero.
	//
	// The reducer MAY keep additional internal state in order to produce
	// patches that combine data from multiple events.
	HandleEvent(s dogma.ProjectionEventScope, m dogma.Message) []Patch

	// MarshalState returns a binary representation of the projection's state.
	//
	// The data MAY include additional internal state that is not present in the
	// State value returned by UnmarshalState().
	MarshalState() ([]byte, error)

	// UnmarshalState restores the projection's state from its binary
	// representation.
	//
	// The data MAY be empty, either because MarshalState() returned empty data
	// or the when the projection is first initialized.
	//
	// The returned State must be isolated from the internal state of the
	// reducer such that it is NOT modified by calls to HandleEvent().
	UnmarshalState(data []byte) (State, error)
}
