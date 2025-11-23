package sqlprojection_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresDriver(t *testing.T) {
	t.Parallel()

	container, err := postgres.Run(
		t.Context(),
		"postgres",
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Log(err)
		}
	})

	dsn, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	runTests(
		t,
		"pgx", dsn,
		PostgresDriver,
	)
}
