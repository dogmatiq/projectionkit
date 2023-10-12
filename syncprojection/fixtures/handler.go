package fixtures

import (
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a test implementation of [await.MessageHandler].
type MessageHandler[S any] struct {
	ConfigureFunc            func(dogma.ProjectionConfigurer)
	NewFunc                  func() S
	RouteEventToInstanceFunc func(dogma.Event) (string, bool)
	HandleEventFunc          func(S, dogma.ProjectionEventScope, dogma.Event) error
}

func (h *MessageHandler[S]) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

func (h *MessageHandler[S]) New() S {
	if h.NewFunc != nil {
		return h.NewFunc()
	}
	var zero S
	return zero
}

func (h *MessageHandler[S]) RouteEventToInstance(e dogma.Event) (string, bool) {
	if h.RouteEventToInstanceFunc != nil {
		return h.RouteEventToInstanceFunc(e)
	}
	return "", false
}

func (h *MessageHandler[S]) HandleEvent(
	state S,
	s dogma.ProjectionEventScope,
	e dogma.Event,
) {
	if h.HandleEventFunc != nil {
		h.HandleEventFunc(state, s, e)
	}
}
