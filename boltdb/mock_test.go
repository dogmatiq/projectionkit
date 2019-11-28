package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	bolt "go.etcd.io/bbolt"
)

// messageHandlerMock is a mock implementation of MessageHandler.
type messageHandlerMock struct {
	ConfigureCall   func(c dogma.ProjectionConfigurer)
	HandleEventCall func(
		ctx context.Context,
		tx *bolt.Tx,
		s dogma.ProjectionEventScope,
		m dogma.Message,
	) error
	TimeoutHintCall func(m dogma.Message) time.Duration
}

func (m *messageHandlerMock) Configure(c dogma.ProjectionConfigurer) {
	if m.ConfigureCall != nil {
		m.ConfigureCall(c)
	}
}

func (m *messageHandlerMock) HandleEvent(
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

func (m *messageHandlerMock) TimeoutHint(msg dogma.Message) time.Duration {
	if m.TimeoutHintCall != nil {
		return m.TimeoutHintCall(msg)
	}

	return time.Duration(0)
}
