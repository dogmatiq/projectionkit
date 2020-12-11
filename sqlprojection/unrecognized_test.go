package sqlprojection_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

var unrecognizedDB = sql.OpenDB(fakeConnector{})

type fakeConnector struct{}

func (fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("fakeConnector: not implemented")
}

func (fakeConnector) Driver() driver.Driver {
	return fakeDriver{}
}

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("fakeDriver: not implemented")
}
