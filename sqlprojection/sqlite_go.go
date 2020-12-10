// +build !cgo

package sqlprojection

func (sqliteDriver) isDup(err error) bool {
	panic("not implemented")
}
