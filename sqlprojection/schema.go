package sqlprojection

import (
	"context"
	"database/sql"
)

// WithoutSchemaCreation is an [Option] that prevents the projection from
// creating its own SQL schema.
func WithoutSchemaCreation() Option {
	return Option{
		applyToAdaptor: func(a *adaptor) {
			a.schemaCreated.Store(true)
		},
	}
}

// CreateSchema creates the schema elements necessary to store projections on
// the given database.
//
// It does not return an error if the schema already exists.
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
// It does not return an error if the schema does not exist.
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
