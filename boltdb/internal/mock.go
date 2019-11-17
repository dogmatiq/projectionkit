package internal

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	bolt "go.etcd.io/bbolt"
)

// MessageHandlerMock is a mock implementation of MessageHandler.
type MessageHandlerMock struct {
	ConfigureCall   func(c dogma.ProjectionConfigurer)
	HandleEventCall func(
		ctx context.Context,
		tx *bolt.Tx,
		s dogma.ProjectionEventScope,
		m dogma.Message,
	) error
	TimeoutHintCall func(m dogma.Message) time.Duration
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
//
// The implementation MUST allow for multiple calls to Configure(). Each
// call SHOULD produce the same configuration.
//
// The engine MUST call Configure() before calling HandleEvent(). It is
// RECOMMENDED that the engine only call Configure() once per handler.
func (m *MessageHandlerMock) Configure(c dogma.ProjectionConfigurer) {
	if m.ConfigureCall != nil {
		m.ConfigureCall(c)
	}
}

// HandleEvent updates the projection to reflect the occurrence of an event.
//
// Changes to the projection state MUST be performed within the supplied
// transaction.
//
// If nil is returned, the projection state has been persisted successfully.
//
// If an error is returned, the projection SHOULD be left in the state it
// was before HandleEvent() was called.
//
// The engine SHOULD provide "at-least-once" delivery guarantees to the
// handler. That is, the engine should call HandleEvent() with the same
// event message until a nil error is returned.
//
// The engine MAY provide guarantees about the order in which event messages
// will be passed to HandleEvent(), however in the interest of engine
// portability the implementation SHOULD NOT assume that HandleEvent() will
// be called with events in the same order that they were recorded.
//
// The supplied context parameter SHOULD have a deadline. The implementation
// SHOULD NOT impose its own deadline. Instead a suitable timeout duration
// can be suggested to the engine via the handler's TimeoutHint() method.
//
// The engine MUST NOT call HandleEvent() with any message of a type that
// has not been configured for consumption by a prior call to Configure().
// If any such message is passed, the implementation MUST panic with the
// UnexpectedMessage value.
//
// The engine MAY call HandleEvent() from multiple goroutines concurrently.
func (m *MessageHandlerMock) HandleEvent(
	ctx context.Context,
	tx *bolt.Tx,
	s dogma.ProjectionEventScope,
	msg dogma.Message,
) error {
	if m.HandleEventCall != nil {
		return m.HandleEventCall(ctx, tx, s, msg)
	}

	return nil
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
//
// The hint SHOULD be as short as possible. The implementation MAY return a
// zero-value to indicate that no hint can be made.
//
// The engine SHOULD use a duration as close as possible to the hint. Use of
// a duration shorter than the hint is NOT RECOMMENDED, as this will likely
// lead to repeated message handling failures.
func (m *MessageHandlerMock) TimeoutHint(msg dogma.Message) time.Duration {
	if m.TimeoutHintCall != nil {
		return m.TimeoutHintCall(msg)
	}

	return time.Duration(0)
}
