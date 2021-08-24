package eventprojection_test

import (
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/projectionkit/eventprojection"
)

// messageHandlerStub is a test implementation of the MessageHandler interface.
type messageHandlerStub struct {
	ConfigureFunc      func(dogma.ProjectionConfigurer)
	HandleEventFunc    func(EventScope, dogma.Message)
	MarshalEventFunc   func(Event) ([]byte, error)
	UnmarshalEventFunc func([]byte) (Event, error)
}

// Configure calls h.ConfigureFunc(c) if h.ConfigureFunc is not nil.
func (h *messageHandlerStub) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

// HandleEvent calls h.HandleEventFunc(s, m) if h.HandleEventFunc is not nil.
func (h *messageHandlerStub) HandleEvent(s EventScope, m dogma.Message) {
	if h.HandleEventFunc != nil {
		h.HandleEventFunc(s, m)
	}
}

// MarshalEvent returns h.MarshalEventFunc(ev) if h.MarshalEventFunc is not nil,
// otherwise it returns (nil, nil).
func (h *messageHandlerStub) MarshalEvent(ev Event) ([]byte, error) {
	if h.MarshalEventFunc != nil {
		return h.MarshalEventFunc(ev)
	}

	return nil, nil
}

// UnmarshalEvent returns h.UnmarshalEventFunc(data) if h.UnmarshalEventFunc is
// not nil, otherwise it returns (nil, nil).
func (h *messageHandlerStub) UnmarshalEvent(data []byte) (Event, error) {
	if h.MarshalEventFunc != nil {
		return h.UnmarshalEventFunc(data)
	}

	return nil, nil
}
