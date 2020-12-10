package sqlprojection_test

import (
	"database/sql"
	"database/sql/driver"
)

type unrecognizedConnector struct {
	driver.Connector
}

func (*unrecognizedConnector) Driver() driver.Driver {
	return &mockDriver{}
}

type mockDriver struct {
	driver.Driver
}

// unrecognizedDB returns a database pool that uses an unrecognized connector.
func unrecognizedDB() *sql.DB {
	return sql.OpenDB(&unrecognizedConnector{})
}
