// +build !cgo

package sqlite

// isDuplicateEntry returns true if err represents a MySQL duplicate entry error.
func isDuplicateEntry(err error) bool {
	panic("not implemented")
}
