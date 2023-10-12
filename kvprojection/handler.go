package kvprojection

import (
	"github.com/dogmatiq/dogma"
)

type MessageHandler interface {
	Configure(c dogma.ProjectionConfigurer)
	RouteEventToKeys(e dogma.Event) [][]byte
	HandleEvent(key, value []byte, s dogma.ProjectionEventScope, e dogma.Event) []byte
	Compact(key, value []byte, s dogma.ProjectionCompactScope) []byte
}
