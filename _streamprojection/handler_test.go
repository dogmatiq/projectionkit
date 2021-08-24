package streamprojection_test

import (
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/projectionkit/streamprojection"
)

type messageHandlerStub struct {
	ConfigureFunc           func(dogma.ProjectionConfigurer)
	RouteEventToStreamsFunc func(dogma.Message) []string
	NewFunc                 func() Snapshot
	HandleEventFunc         func(Snapshot, EventScope, dogma.Message)
}

func (h *messageHandlerStub) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

func (h *messageHandlerStub) New() Snapshot {
	if h.NewFunc != nil {
		return h.NewFunc()
	}

	panic("not implemented")
}

func (h *messageHandlerStub) RouteEventToStreams(m dogma.Message) []string {
	if h.RouteEventToStreamsFunc != nil {
		return h.RouteEventToStreamsFunc(m)
	}

	return nil
}

func (h *messageHandlerStub) HandleEvent(
	sn Snapshot,
	s EventScope,
	m dogma.Message,
) {
	if h.HandleEventFunc != nil {
		h.HandleEventFunc(sn, s, m)
	}
}
