package neo4jprojection

import (
	"context"

	"github.com/dogmatiq/projectionkit/resource"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in a Neo4j database.
type ResourceRepository struct {
	db  neo4j.DriverWithContext
	key string
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new Neo4j resource repository.
func NewResourceRepository(
	db neo4j.DriverWithContext,
	key string,
) *ResourceRepository {
	return &ResourceRepository{db, key}
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteRead(ctx, func(mtx neo4j.ManagedTransaction) (any, error) {

		result, err := mtx.Run(ctx,
			`MATCH (o:OCC{handler: $handler, resource: $resource}
			RETURN o.version`,
			map[string]any{
				"handler":  rr.key,
				"resource": r,
			},
		)

		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return nil, err

}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(mtx neo4j.ManagedTransaction) (any, error) {
		result, err := mtx.Run(ctx,
			`CREATE (o:OCC {handler: $handler, resource: $resource})
			SET o.version = $version
			RETURN o`,
			map[string]any{
				"version":  v,
				"handler":  rr.key,
				"resource": r,
			})
		if err != nil {
			return nil, err
		}

		return nil, result.Err()
	})
	if err != nil {
		return err
	}

	return nil
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	res, err := session.ExecuteWrite(ctx, func(mtx neo4j.ManagedTransaction) (any, error) {
		result, err := mtx.Run(ctx,
			`MATCH (o:OCC {handler: $handler, resource: $resource, version: $version})
			SET o.resource = $resource
			RETURN o`,
			map[string]any{
				"version":  n,
				"handler":  rr.key,
				"resource": r,
			})
		if err != nil {
			return nil, err
		}
		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}
		return nil, nil
	})
	if err != nil {
		return false, err
	}

	if res == nil {
		return false, nil
	}

	return true, nil
}

// UpdateResourceVersion updates the version of the resource r to n and performs
// a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, neo4j.ExplicitTransaction) (bool, error),
) (ok bool, err error) {

	return rr.withTx(ctx, func(tx neo4j.ExplicitTransaction) (bool, error) {
		var err error
		result, err := tx.Run(ctx,
			`MATCH (o:OCC {handler: $handler, resource: $resource, version: $current_version})
			SET o.version = $new_version
			RETURN o`,
			map[string]any{
				"current_version": c,
				"new_version":     n,
				"handler":         rr.key,
				"resource":        r,
			},
		)
		if err != nil {
			return false, err
		}
		if result.Next(ctx) {
			return fn(ctx, tx)
		}

		return false, nil
	})
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx,
			`MATCH (o:OCC {handler: $handler, resource: $resource})
			DELETE o`,
			map[string]interface{}{
				"handler":  rr.key,
				"resource": r,
			})
		if err != nil {
			return nil, err
		}

		return result.Consume(ctx)
	})
	if err != nil {
		return err
	}

	return nil

}

// withTx calls fn on rr.db.
//
// fn is called within a transaction. The transaction is committed if fn returns
// ok; otherwise, it is rolled back.
func (rr *ResourceRepository) withTx(
	ctx context.Context,
	fn func(neo4j.ExplicitTransaction) (bool, error),
) (bool, error) {
	var ok bool

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) // nolint:errcheck

	ok, err = fn(tx)
	if err != nil {
		return false, err
	}

	if ok {
		return true, tx.Commit(ctx)
	}

	return false, tx.Rollback(ctx)
}

var (
	// node is the node that contains all data related to projection OCC.
	node = []byte("projection_occ")
)
