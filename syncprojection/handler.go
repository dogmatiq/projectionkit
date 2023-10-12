package syncprojection

import (
	"github.com/dogmatiq/dogma"
)

// MessageHandler is a specialization of [dogma.ProjectionMessageHandler] that
// updates in memory state of type S.
type MessageHandler[S any] interface {
	Configure(c dogma.ProjectionConfigurer)
	New() S
	RouteEventToInstance(dogma.Event) (string, bool)
	HandleEvent(S, dogma.ProjectionEventScope, dogma.Event)
}
