package sqlprojection

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements necessary to store projections on
// the given database.
//
// If no candidate drivers are provided all built-in drivers are considered as
// candidates.
func CreateSchema(ctx context.Context, db *sql.DB, options ...Option) error {
	var cs candidateSet
	cs.init(db, options)

	d, err := cs.resolve(ctx)
	if err != nil {
		return err
	}

	return d.CreateSchema(ctx, db)
}

// DropSchema drops the schema elements necessary to store projections on the
// given database.
//
// If no candidate drivers are provided all built-in drivers are considered as
// candidates.
func DropSchema(ctx context.Context, db *sql.DB, options ...Option) error {
	var cs candidateSet
	cs.init(db, options)

	d, err := cs.resolve(ctx)
	if err != nil {
		return err
	}

	return d.DropSchema(ctx, db)
}
