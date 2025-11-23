package syncx_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/dogmatiq/projectionkit/internal/syncx"
)

func TestSucceedOnce(t *testing.T) {
	t.Run("it does not re-run after success", func(t *testing.T) {
		var once SucceedOnce

		if err := once.Do(
			t.Context(),
			func(context.Context) error {
				return nil
			},
		); err != nil {
			t.Fatal(err)
		}

		once.Do(
			t.Context(),
			func(context.Context) error {
				t.Fatal("unexpected call")
				return nil
			},
		)
	})

	t.Run("it re-runs after failure", func(t *testing.T) {
		var once SucceedOnce
		want := errors.New("<error>")

		if got := once.Do(
			t.Context(),
			func(context.Context) error {
				return want
			},
		); got != want {
			t.Fatalf("unexpected error: got %v, want %v", got, want)
		}

		called := false
		if err := once.Do(
			t.Context(),
			func(context.Context) error {
				called = true
				return nil
			},
		); err != nil {
			t.Fatal(err)
		}

		if !called {
			t.Fatal("expected function to be called")
		}
	})
}
