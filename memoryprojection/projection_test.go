package memoryprojection_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/handlertest"
	"github.com/dogmatiq/projectionkit/memoryprojection"
	. "github.com/dogmatiq/projectionkit/memoryprojection"
	"github.com/dogmatiq/projectionkit/memoryprojection/internal/fixtures" // can't dot-import due to conflict
)

func TestProjection(t *testing.T) {
	setup := func(t *testing.T) (deps struct {
		Handler *fixtures.MessageHandler[int]
		Adaptor *Projection[int, *fixtures.MessageHandler[int]]
	}) {
		t.Helper()

		deps.Handler = &fixtures.MessageHandler[int]{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", handlertest.IdentityKey)
			},
		}

		deps.Adaptor = &Projection[int, *fixtures.MessageHandler[int]]{
			Handler: deps.Handler,
		}

		return deps
	}

	handlertest.Run(
		t,
		func(t *testing.T) dogma.ProjectionMessageHandler {
			return setup(t).Adaptor
		},
	)

	t.Run("when there is no state", func(t *testing.T) {
		t.Run("func HandleEvent()", func(t *testing.T) {
			t.Run("it forwards a zero value to the handler", func(t *testing.T) {
				deps := setup(t)

				called := false
				deps.Handler.HandleEventFunc = func(
					v int,
					_ dogma.ProjectionEventScope,
					m dogma.Event,
				) (int, error) {
					called = true

					if want := 0; v != want {
						t.Fatalf("unexpected value: got %d, want %d", v, want)
					}

					if want := EventA1; m != want {
						t.Fatalf("unexpected event: got %v, want %v", m, want)
					}

					return v, nil
				}

				got, err := deps.Adaptor.HandleEvent(
					t.Context(),
					&ProjectionEventScopeStub{},
					EventA1,
				)
				if err != nil {
					t.Fatal(err)
				}

				if want := uint64(1); got != want {
					t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
				}

				if !called {
					t.Fatal("expected handler to be called")
				}
			})
		})

		t.Run("func Compact()", func(t *testing.T) {
			t.Run("it does not forward to the handler", func(t *testing.T) {
				deps := setup(t)

				deps.Handler.CompactFunc = func(
					int,
					dogma.ProjectionCompactScope,
				) int {
					t.Fatal("unexpected call")
					return 0
				}

				if err := deps.Adaptor.Compact(
					context.Background(),
					&ProjectionCompactScopeStub{},
				); err != nil {
					t.Fatal(err)
				}
			})
		})

		t.Run("func Query()", func(t *testing.T) {
			t.Run("it calls the query function with a zero value", func(t *testing.T) {
				deps := setup(t)

				got := memoryprojection.Query(
					deps.Adaptor,
					func(v int) int {
						return v * 2
					},
				)

				if want := 0; got != want {
					t.Fatalf("unexpected query result: got %d, want %d", got, want)
				}
			})
		})
	})

	t.Run("when there is existing state", func(t *testing.T) {
		setup := func(t *testing.T) struct {
			Handler *fixtures.MessageHandler[int]
			Adaptor *Projection[int, *fixtures.MessageHandler[int]]
		} {
			deps := setup(t)

			deps.Handler.HandleEventFunc = func(
				v int,
				_ dogma.ProjectionEventScope,
				_ dogma.Event,
			) (int, error) {
				return 321, nil
			}

			if _, err := deps.Adaptor.HandleEvent(
				t.Context(),
				&ProjectionEventScopeStub{},
				EventA1,
			); err != nil {
				t.Fatal(err)
			}

			return deps
		}

		t.Run("func HandleEvent()", func(t *testing.T) {
			t.Run("it forwards the existing value to the handler", func(t *testing.T) {
				deps := setup(t)

				called := false
				deps.Handler.HandleEventFunc = func(
					v int,
					_ dogma.ProjectionEventScope,
					m dogma.Event,
				) (int, error) {
					called = true

					if want := 321; v != want {
						t.Fatalf("unexpected value: got %d, want %d", v, want)
					}

					return v, nil
				}

				got, err := deps.Adaptor.HandleEvent(
					t.Context(),
					&ProjectionEventScopeStub{
						OffsetFunc:           func() uint64 { return 1 },
						CheckpointOffsetFunc: func() uint64 { return 1 },
					},
					EventA1,
				)
				if err != nil {
					t.Fatal(err)
				}

				if want := uint64(2); got != want {
					t.Fatalf("unexpected checkpoint offset: got %d, want %d", got, want)
				}

				if !called {
					t.Fatal("expected handler to be called")
				}
			})
		})

		t.Run("func Compact()", func(t *testing.T) {
			t.Run("it forwards to the handler", func(t *testing.T) {
				deps := setup(t)

				called := false
				deps.Handler.CompactFunc = func(
					v int,
					_ dogma.ProjectionCompactScope,
				) int {
					called = true

					if want := 321; v != want {
						t.Fatalf("unexpected value: got %d, want %d", v, want)
					}

					return v
				}

				if err := deps.Adaptor.Compact(
					context.Background(),
					&ProjectionCompactScopeStub{},
				); err != nil {
					t.Fatal(err)
				}

				if !called {
					t.Fatal("expected handler to be called")
				}
			})
		})

		t.Run("func Query()", func(t *testing.T) {
			t.Run("it calls the query function with the existing value", func(t *testing.T) {
				deps := setup(t)

				got := memoryprojection.Query(
					deps.Adaptor,
					func(v int) int {
						return v * 2
					},
				)

				if want := 642; got != want {
					t.Fatalf("unexpected query result: got %d, want %d", got, want)
				}
			})
		})

	})
}
