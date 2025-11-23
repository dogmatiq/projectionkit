package handlertest

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/identity"
)

// IdentityKey is the expected identity key for handlers used in these tests.
const IdentityKey = "26902c80-a1b8-43d1-99ae-ea5651656e63"

// Run runs generic behavioral tests for a handler implementation.
func Run(
	t *testing.T,
	setup func(t *testing.T) dogma.ProjectionMessageHandler,
) {
	t.Run("func Configure()", func(t *testing.T) {
		t.Run("it returns the expected identity", func(t *testing.T) {
			handler := setup(t)

			got := identity.Key(handler)
			want := [16]byte{
				0x26, 0x90, 0x2c, 0x80,
				0xa1, 0xb8, 0x43, 0xd1,
				0x99, 0xae, 0xea, 0x56,
				0x51, 0x65, 0x6e, 0x63,
			}

			if got != want {
				t.Fatalf("unexpected identity: got %v, want %v", got, want)
			}
		})
	})

	t.Run("func HandleEvent()", func(t *testing.T) {
		t.Run("it returns the new checkpoint offset", func(t *testing.T) {
			handler := setup(t)

			got, err := handler.HandleEvent(
				t.Context(),
				&stubs.ProjectionEventScopeStub{},
				stubs.EventA1,
			)
			if err != nil {
				t.Fatalf("unable to handle first event: %s", err)
			}

			if want := uint64(1); got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}

			got, err = handler.HandleEvent(
				t.Context(),
				&stubs.ProjectionEventScopeStub{
					OffsetFunc:           func() uint64 { return 1 },
					CheckpointOffsetFunc: func() uint64 { return 1 },
				},
				stubs.EventA2,
			)
			if err != nil {
				t.Fatalf("unable to handle second event: %s", err)
			}

			if want := uint64(2); got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}
		})

		t.Run("it returns the actual checkpoint offset if the provided checkpoint offset is not current", func(t *testing.T) {
			handler := setup(t)
			scope := &stubs.ProjectionEventScopeStub{}
			want := uint64(1)

			got, err := handler.HandleEvent(
				t.Context(),
				scope,
				stubs.EventA1,
			)
			if err != nil {
				t.Fatalf("unable to handle first event: %s", err)
			}

			if got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}

			scope.CheckpointOffsetFunc = func() uint64 {
				return 123
			}

			got, err = handler.HandleEvent(
				t.Context(),
				scope,
				stubs.EventA2,
			)
			if err != nil {
				t.Fatalf("unable to handle second event: %s", err)
			}

			if got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}

			// Ensure that the checkpoint offset was not updated.
			got, err = handler.CheckpointOffset(
				t.Context(),
				scope.StreamID(),
			)
			if err != nil {
				t.Fatalf("unable to load checkpoint offset: %s", err)
			}

			if got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}
		})
	})

	t.Run("func CheckpointOffset()", func(t *testing.T) {
		t.Run("it returns the checkpoint offset", func(t *testing.T) {
			handler := setup(t)
			scope := &stubs.ProjectionEventScopeStub{}

			want, err := handler.HandleEvent(
				t.Context(),
				scope,
				stubs.EventA1,
			)
			if err != nil {
				t.Fatalf("unable to handle event: %s", err)
			}

			got, err := handler.CheckpointOffset(
				t.Context(),
				scope.StreamID(),
			)
			if err != nil {
				t.Fatalf("unable to load checkpoint offset: %s", err)
			}

			if got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}
		})

		t.Run("it returns 0 if no events from the stream have been applied", func(t *testing.T) {
			handler := setup(t)

			got, err := handler.CheckpointOffset(
				t.Context(),
				"e108b1d5-f2c2-44f1-884d-a5cdc1d575f0",
			)
			if err != nil {
				t.Fatalf("unable to load checkpoint offset: %s", err)
			}

			if want := uint64(0); got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}
		})
	})

	t.Run("func Compact()", func(t *testing.T) {
		t.Run("it does not return an error", func(t *testing.T) {
			handler := setup(t)

			if err := handler.Compact(
				t.Context(),
				&stubs.ProjectionCompactScopeStub{},
			); err != nil {
				t.Fatalf("unable to compact projection: %s", err)
			}
		})
	})

	t.Run("func Reset()", func(t *testing.T) {
		t.Run("it resets the checkpoint offsets", func(t *testing.T) {
			handler := setup(t)
			scope := &stubs.ProjectionEventScopeStub{}

			if _, err := handler.HandleEvent(
				t.Context(),
				scope,
				stubs.EventA1,
			); err != nil {
				t.Fatalf("unable to handle event: %s", err)
			}

			if err := handler.Reset(
				t.Context(),
				&stubs.ProjectionResetScopeStub{},
			); err != nil {
				t.Fatalf("unable to reset projection: %s", err)
			}

			got, err := handler.CheckpointOffset(
				t.Context(),
				scope.StreamID(),
			)
			if err != nil {
				t.Fatalf("unable to load checkpoint offset: %s", err)
			}

			if want := uint64(0); got != want {
				t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
			}
		})

		t.Run("can be called when the projection is empty", func(t *testing.T) {
			handler := setup(t)

			if err := handler.Reset(
				t.Context(),
				&stubs.ProjectionResetScopeStub{},
			); err != nil {
				t.Fatalf("unable to reset projection: %s", err)
			}
		})

		t.Run("can be called when the projection has already been reset", func(t *testing.T) {
			handler := setup(t)

			if _, err := handler.HandleEvent(
				t.Context(),
				&stubs.ProjectionEventScopeStub{},
				stubs.EventA1,
			); err != nil {
				t.Fatalf("unable to handle event: %s", err)
			}

			if err := handler.Reset(
				t.Context(),
				&stubs.ProjectionResetScopeStub{},
			); err != nil {
				t.Fatalf("unable to reset projection the first time: %s", err)
			}

			if err := handler.Reset(
				t.Context(),
				&stubs.ProjectionResetScopeStub{},
			); err != nil {
				t.Fatalf("unable to reset projection the second time: %s", err)
			}
		})

	})
}
