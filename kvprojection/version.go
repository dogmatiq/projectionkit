package kvprojection

import (
	"bytes"
	"context"
)

// storeVersion unconditionally updates a resource's version within a
// transaction.
//
// It deletes the resource from the database if v is empty.
func storeVersion(
	ctx context.Context,
	tx Tx,
	ks string,
	r, v []byte,
) error {
	if len(v) == 0 {
		// If the version is empty, we can delete the key.
		return tx.Delete(ctx, ks, r)
	}

	return tx.Set(ctx, ks, r, v)
}

// updateVersion updates a resource's version within a transaction.
//
// It deletes the resource from the database if n is empty.
//
// It returns false if the current version c does not match the version in the
// database.
func updateVersion(
	ctx context.Context,
	tx Tx,
	ks string,
	r, c, n []byte,
) (bool, error) {
	x, err := tx.Get(ctx, ks, r)
	if err != nil {
		return false, err
	}

	// If the "current" version is different to the value associated with the
	// resource's key, that means the current version was not correct.
	if !bytes.Equal(x, c) {
		return false, nil
	}

	if len(n) == 0 {
		// If the "next" version is empty, we can delete the key.
		return true, tx.Delete(ctx, ks, r)
	}

	return true, tx.Set(ctx, ks, r, n)
}

// queryVersion returns the current version of a resource from the database.
//
// It returns nil if there is no version persisted for the resource.
func queryVersion(
	ctx context.Context,
	db DB,
	ks string,
	r []byte,
) ([]byte, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return tx.Get(ctx, ks, r)
}

// deleteResource ensures that a resource does not exist in the database.
func deleteResource(
	ctx context.Context,
	db DB,
	ks string,
	r []byte,
) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.Delete(ctx, ks, r); err != nil {
		return err
	}

	return tx.Commit()
}
