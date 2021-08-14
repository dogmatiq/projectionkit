package eventprojection

import (
	"time"

	"github.com/dogmatiq/dogma"
)

// ProjectionEventScope is a specialization of dogma.ProjectionEventScope for
// stream projections.
type EventScope interface {
	dogma.ProjectionEventScope

	// RecordEvent records the occurrence of an event to a stream as a result of
	// the Dogma event message that is being handled.
	//
	// Any prior call to Close() for the same stream within the same scope is
	// negated.
	RecordEvent(stream string, ev Event)

	// Close indicates to the engine that a stream has ended an no more events
	// will be recorded to it.
	//
	// r is the time after which the events on this stream will no longer be
	// retained.
	//
	// A call to Close() is negated by a subsequent call to RecordEvent() for
	// the same stream within the same scope.
	Close(stream string, r time.Time)
}

// scope is the default implementation of the EventScope interface.
type scope struct {
	dogma.ProjectionEventScope
	changes map[string]*StreamChanges
}

type StreamChanges struct {
	Events      []Event
	Closed      bool
	RetainUntil time.Time
}

func (s *scope) RecordEvent(stream string, ev Event) {
	ch := s.get(stream)
	ch.Closed = false
	ch.Events = append(ch.Events, ev)
}

func (s *scope) Close(stream string, r time.Time) {
	ch := s.get(stream)
	ch.Closed = true
	ch.RetainUntil = r
}

func (s *scope) get(stream string) *StreamChanges {
	if c, ok := s.changes[stream]; ok {
		return c
	}

	if s.changes == nil {
		s.changes = map[string]*StreamChanges{}
	}

	c := &StreamChanges{}
	s.changes[stream] = c

	return c
}
