package drivertest

import (
	"database/sql"
	"database/sql/driver"
)

// MockConnector is mock of the driver.Connector interface.
type MockConnector struct {
	driver.Connector
}

// Driver returns the connector's driver.
func (*MockConnector) Driver() driver.Driver {
	return &MockDriver{}
}

// MockDriver is mock of the driver.Driver interface.
type MockDriver struct {
	driver.Driver
}

// MockDB returns a database pool that uses the mock connector.
func MockDB() *sql.DB {
	return sql.OpenDB(&MockConnector{})
}
