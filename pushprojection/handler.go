package pushprojection

import (
	"github.com/dogmatiq/dogma"
)

type Update[T any] interface {
	ApplyTo(T)
}

type MessageHandler[T any] interface {
	Configure(c dogma.ProjectionConfigurer)
	New() T
	RouteEventToPartitions(dogma.Event) []string
	HandleEvent(
		partition string,
		state T,
		s dogma.ProjectionEventScope,
		e dogma.Event,
	) []Update[T]
}
