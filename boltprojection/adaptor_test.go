package boltprojection_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/boltprojection"
	"github.com/dogmatiq/projectionkit/boltprojection/internal/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"go.etcd.io/bbolt"
)

func TestAdaptor(t *testing.T) {
	setup := func(t *testing.T) (deps struct {
		DB      *bbolt.DB
		Handler *fixtures.MessageHandler
		Adaptor dogma.ProjectionMessageHandler
	}) {
		t.Helper()

		tmp, err := os.CreateTemp("", "*.boltdb")
		if err != nil {
			t.Fatal(err)
		}
		tmp.Close()

		t.Cleanup(func() {
			os.Remove(tmp.Name())
		})

		deps.DB, err = bbolt.Open(tmp.Name(), 0600, bbolt.DefaultOptions)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			deps.DB.Close()
		})

		deps.Handler = &fixtures.MessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", "87bb65eb-a5db-4213-8f5c-4ddbc97aa711")
			},
		}

		deps.Adaptor = New(deps.DB, deps.Handler)

		return deps
	}

	t.Run("common adaptor behavior", func(t *testing.T) {
		deps := setup(t)
		adaptortest.TestAdaptor(t, deps.Adaptor)
	})

	t.Run("func Configure()", func(t *testing.T) {
		t.Run("it forwards to the handler", func(t *testing.T) {
			deps := setup(t)

			got := identity.Key(deps.Adaptor).AsString()
			want := "87bb65eb-a5db-4213-8f5c-4ddbc97aa711"

			if got != want {
				t.Fatalf("unexpected identity: got %q, want %q", got, want)
			}
		})
	})

	t.Run("func HandleEvent()", func(t *testing.T) {
		t.Run("it returns an error if the application's message handler fails", func(t *testing.T) {
			deps := setup(t)

			want := errors.New("handle event test error")

			deps.Handler.HandleEventFunc = func(
				context.Context,
				*bbolt.Tx,
				dogma.ProjectionEventScope,
				dogma.Event,
			) error {
				return want
			}

			_, got := deps.Adaptor.HandleEvent(
				t.Context(),
				&ProjectionEventScopeStub{},
				EventA1,
			)
			if got != want {
				t.Fatalf("unexpected error: got %v, want %v", got, want)
			}
		})
	})

	t.Run("func Compact()", func(t *testing.T) {
		t.Run("it forwards to the handler", func(t *testing.T) {
			deps := setup(t)
			want := errors.New("<error>")

			deps.Handler.CompactFunc = func(
				_ context.Context,
				db *bbolt.DB,
				_ dogma.ProjectionCompactScope,
			) error {
				if db != deps.DB {
					t.Fatalf("unexpected DB: got %p, want %p", db, deps.DB)
				}
				return want
			}

			got := deps.Adaptor.Compact(
				t.Context(),
				nil, // scope
			)
			if got != want {
				t.Fatalf("unexpected error: got %v, want %v", got, want)
			}
		})
	})
}
