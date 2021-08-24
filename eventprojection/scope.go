package eventprojection

import (
	"time"

	"github.com/dogmatiq/dogma"
)

// EventScope is a specialization of dogma.ProjectionEventScope for event
// projections.
type EventScope interface {
	dogma.ProjectionEventScope

	// RecordEvent records the occurrence of an event to a stream as a result of
	// the Dogma event message that is being handled.
	//
	// streamID is an application-defined identifier for an ordered stream of
	// events. Streams are created the first time an event is recorded to them.
	//
	// Any prior call to Close() for the same stream within the same scope is
	// negated.
	RecordEvent(streamID string, ev Event)

	// CloseStream indicates to the engine that a stream has ended an no more
	// events will be recorded to it.
	//
	// expireAt is the time after which the stream will be removed from the
	// projection.
	//
	// A call to Close() is negated by a subsequent call to RecordEvent() for
	// the same stream within the same scope.
	CloseStream(streamID string, expireAt time.Time)
}

// streamChanges represents the changes made to a stream within a scope.
type streamChanges struct {
	Events   []Event
	ExpireAt *time.Time
}

// scope is the default implementation of the EventScope interface.
type scope struct {
	dogma.ProjectionEventScope
	changes map[string]*streamChanges
}

func (s *scope) RecordEvent(streamID string, ev Event) {
	ch := s.getChanges(streamID)
	ch.ExpireAt = nil
	ch.Events = append(ch.Events, ev)
}

func (s *scope) CloseStream(streamID string, expireAt time.Time) {
	ch := s.getChanges(streamID)
	ch.ExpireAt = &expireAt
}

func (s *scope) getChanges(streamID string) *streamChanges {
	if c, ok := s.changes[streamID]; ok {
		return c
	}

	if s.changes == nil {
		s.changes = map[string]*streamChanges{}
	}

	c := &streamChanges{}
	s.changes[streamID] = c

	return c
}
