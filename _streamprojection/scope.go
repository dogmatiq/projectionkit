package streamprojection

import (
	"time"

	"github.com/dogmatiq/dogma"
)

// ProjectionEventScope is a specialization of dogma.ProjectionEventScope for
// stream projections.
type EventScope interface {
	dogma.ProjectionEventScope

	// StreamID returns the ID of the targetted stream.
	StreamID() string

	// RecordEvent records the occurrence of an event to the stream as a result
	// of the Dogma event message that is being handled.
	//
	// The engine MUST call ApplyEvent(ev) on the snapshot that was passed
	// to HandleEvent(), such that the applied changes are visible to the
	// handler after RecordEvent() returns.
	//
	// Any prior call to Close() within the same scope is negated.
	RecordEvent(ev Event)

	// Close indicates to the engine that the stream has ended an no more events
	// will be recorded.
	//
	// r specifies the time at which the stream's most recent snapshot will
	// cease being available to consumers.
	//
	// A call to Close() is negated by a subsequent call to RecordEvent() within
	// the same scope.
	//
	// The engine MUST pass a newly initialized snapshot to the handler when the
	// next Dogma event message is handled.
	Close(r time.Time)
}

// scope is the default implementation of the EventScope interface.
type scope struct {
	dogma.ProjectionEventScope

	id          string
	snapshot    Snapshot
	events      []Event
	closed      bool
	retainUntil time.Time
}

func (s *scope) StreamID() string {
	return s.id
}

func (s *scope) RecordEvent(ev Event) {
	s.closed = false
	s.events = append(s.events, ev)
	s.snapshot.ApplyEvent(ev)
}

func (s *scope) Close(r time.Time) {
	s.closed = true
	s.retainUntil = r
}
