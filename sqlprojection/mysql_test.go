package sqlprojection_test

import (
	"context"
	"testing"

	. "github.com/dogmatiq/projectionkit/sqlprojection"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func TestMySQLDriver_withMySQL(t *testing.T) {
	t.Parallel()

	container, err := mysql.Run(
		t.Context(),
		"mysql",
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
		"mysql", dsn,
		MySQLDriver,
	)
}

func TestMySQLDriver_withMariaDB(t *testing.T) {
	t.Parallel()

	container, err := mariadb.Run(
		t.Context(),
		"mariadb",
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
		"mysql", dsn,
		MySQLDriver,
	)
}
