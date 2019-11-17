package internal

import (
	"io/ioutil"
	"os"

	"github.com/onsi/gomega"
	bolt "go.etcd.io/bbolt"
)

// TempDB is used to create a temporary instance of BoltDB to be used for
// testing.
type TempDB struct {
	DB *bolt.DB

	fp string
}

// NewTempDB creates and returns an instance of TempDB.
func NewTempDB() *TempDB {
	t := &TempDB{}
	f, err := ioutil.TempFile("", "*.boltdb")
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	t.fp = f.Name()

	t.DB, err = bolt.Open(
		t.fp,
		0600,
		&bolt.Options{
			OpenFile: func(
				string,
				int,
				os.FileMode,
			) (*os.File, error) {
				return f, nil
			},
		},
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return t
}

// Close closes the temporary BoltDB database and removes the database file.
func (t *TempDB) Close() {
	err := t.DB.Close()
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = os.Remove(t.fp)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}
