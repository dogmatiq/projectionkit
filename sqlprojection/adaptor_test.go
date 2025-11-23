package sqlprojection_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/handlertest"
	. "github.com/dogmatiq/projectionkit/sqlprojection"
	"github.com/dogmatiq/projectionkit/sqlprojection/internal/fixtures" // can't dot-import due to conflict
)

func runTests(
	t *testing.T,
	driverName, dsn string,
	driver Driver,
) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatalf("cannot open test database: %s", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	setup := func(t *testing.T) (deps struct {
		Handler *fixtures.MessageHandler
		Adaptor dogma.ProjectionMessageHandler
	}) {
		t.Helper()

		if err := driver.CreateSchema(t.Context(), db); err != nil {
			t.Fatalf("cannot create schema: %s", err)
		}

		t.Cleanup(func() {
			if err := driver.DropSchema(context.Background(), db); err != nil {
				t.Fatalf("cannot drop schema: %s", err)
			}
		})

		deps.Handler = &fixtures.MessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", handlertest.IdentityKey)
			},
		}

		deps.Adaptor = New(db, driver, deps.Handler)

		return deps
	}

	handlertest.Run(
		t,
		func(t *testing.T) dogma.ProjectionMessageHandler {
			return setup(t).Adaptor
		},
	)

	t.Run("func HandleEvent()", func(t *testing.T) {
		t.Run("it forwards to the handler", func(t *testing.T) {
			deps := setup(t)
			want := errors.New("<error>")

			deps.Handler.HandleEventFunc = func(
				context.Context,
				*sql.Tx,
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
				d *sql.DB,
				_ dogma.ProjectionCompactScope,
			) error {
				if d != db {
					t.Fatal("received unexpected db instance")
				}
				return want
			}

			got := deps.Adaptor.Compact(
				t.Context(),
				&ProjectionCompactScopeStub{},
			)

			if got != want {
				t.Fatalf("unexpected error: got %v, want %v", got, want)
			}
		})
	})

	t.Run("schema management", func(t *testing.T) {
		t.Run("func CreateSchema()", func(t *testing.T) {
			t.Run("it can be called when the schema already exists", func(t *testing.T) {
				if err := driver.CreateSchema(t.Context(), db); err != nil {
					t.Fatalf("unable to create schema the first time: %s", err)
				}

				if err := driver.CreateSchema(t.Context(), db); err != nil {
					t.Fatalf("unable to create schema the second time: %s", err)
				}
			})
		})

		t.Run("func DropSchema()", func(t *testing.T) {
			t.Run("it can be called when the schema does not exist", func(t *testing.T) {
				if err := driver.DropSchema(t.Context(), db); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("it can be called when the schema has already been dropped", func(t *testing.T) {
				if err := driver.CreateSchema(t.Context(), db); err != nil {
					t.Fatalf("unable to create schema: %s", err)
				}

				if err := driver.DropSchema(t.Context(), db); err != nil {
					t.Fatalf("unable to drop schema the first time: %s", err)
				}

				if err := driver.DropSchema(t.Context(), db); err != nil {
					t.Fatalf("unable to drop schema the second time: %s", err)
				}
			})
		})
	})
}
