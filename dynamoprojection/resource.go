package dynamoprojection

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/awsx"
	"github.com/dogmatiq/projectionkit/resource"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in an AWS DynamoDB database.
type ResourceRepository struct {
	db         *dynamodb.DynamoDB
	key        string
	occTable   string
	decorators *decorators
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new DynamoDB resource repository.
func NewResourceRepository(
	db *dynamodb.DynamoDB,
	key, occTable string,
	options ...ResourceRepositoryOption,
) *ResourceRepository {
	r := &ResourceRepository{
		db:         db,
		key:        key,
		occTable:   occTable,
		decorators: &decorators{},
	}

	for _, opt := range options {
		opt.applyResourceRepositoryOption(r.decorators)
	}

	return r
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	out, err := awsx.Do(
		ctx,
		rr.db.GetItemWithContext,
		rr.decorators.decorateGetItem,
		&dynamodb.GetItemInput{
			TableName: aws.String(rr.occTable),
			Key: map[string]*dynamodb.AttributeValue{
				handlerAndResourceAttr: {
					B: handlerAndResource(rr.key, r),
				},
			},
		},
	)

	if out.Item == nil || err != nil {
		return nil, err
	}

	return out.Item[resourceVersionAttr].B, nil
}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {
	// Normalize an empty version to always be an empty byte slice.
	if len(v) == 0 {
		v = []byte{}
	}

	_, err := awsx.Do(
		ctx,
		rr.db.PutItemWithContext,
		rr.decorators.decoratePutItem,
		&dynamodb.PutItemInput{
			TableName: aws.String(rr.occTable),
			Item: map[string]*dynamodb.AttributeValue{
				handlerAndResourceAttr: {
					B: handlerAndResource(rr.key, r),
				},
				resourceVersionAttr: {
					B: v,
				},
			},
		},
	)

	return err
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, _ error) {
	return rr.UpdateResourceVersionAndTransactionItems(ctx, r, c, n)
}

// UpdateResourceVersionAndTransactionItems updates the version of the resource
// r to n and the given items within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionAndTransactionItems(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (ok bool, err error) {
	if len(c) == 0 {
		return rr.createResourceWithinTx(ctx, r, c, n, items...)
	}

	if len(n) == 0 {
		return rr.deleteResourceWithinTx(ctx, r, c, n, items...)
	}

	return rr.updateResourceWithinTx(ctx, r, c, n, items...)
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {
	_, err := awsx.Do(
		ctx,
		rr.db.DeleteItemWithContext,
		rr.decorators.decorateDeleteItem,
		&dynamodb.DeleteItemInput{
			TableName: aws.String(rr.occTable),
			Key: map[string]*dynamodb.AttributeValue{
				handlerAndResourceAttr: {
					B: handlerAndResource(rr.key, r),
				},
			},
		},
	)

	return err
}

// createResourceWithinTx creates a resource record in the projection OCC table
// and applies the supplied items within a single transaction.
func (rr *ResourceRepository) createResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.db.TransactWriteItemsWithContext,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				items,
				&dynamodb.TransactWriteItem{
					Put: &dynamodb.Put{
						TableName:           aws.String(rr.occTable),
						ConditionExpression: aws.String(`attribute_not_exists(#HR)`),
						ExpressionAttributeNames: map[string]*string{
							"#HR": aws.String(handlerAndResourceAttr),
						},
						Item: map[string]*dynamodb.AttributeValue{
							handlerAndResourceAttr: {
								B: handlerAndResource(rr.key, r),
							},
							resourceVersionAttr: {
								B: n,
							},
						},
					},
				},
			),
		},
	)

	if awsx.IsErrorCode(err, dynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}

	return err == nil, err
}

// deleteResourceWithinTx deletes a resource record in the projection OCC table
// and applies the supplied items within a single transaction.
func (rr *ResourceRepository) deleteResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.db.TransactWriteItemsWithContext,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				items,
				&dynamodb.TransactWriteItem{
					Delete: &dynamodb.Delete{
						TableName:           aws.String(rr.occTable),
						ConditionExpression: aws.String(`#C = :C`),
						ExpressionAttributeNames: map[string]*string{
							"#C": aws.String(resourceVersionAttr),
						},
						ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
							":C": {
								B: c,
							},
						},
						Key: map[string]*dynamodb.AttributeValue{
							handlerAndResourceAttr: {
								B: handlerAndResource(rr.key, r),
							},
						},
					},
				},
			),
		},
	)

	if awsx.IsErrorCode(err, dynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}

	return err == nil, err
}

// updateResourceWithinTx updates a resource record in the projection OCC table
// and applies the supplied items within a single transaction.
func (rr *ResourceRepository) updateResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.db.TransactWriteItemsWithContext,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				items,
				&dynamodb.TransactWriteItem{
					Put: &dynamodb.Put{
						TableName:           aws.String(rr.occTable),
						ConditionExpression: aws.String(`#C = :C`),
						ExpressionAttributeNames: map[string]*string{
							"#C": aws.String(resourceVersionAttr),
						},
						ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
							":C": {
								B: c,
							},
						},
						Item: map[string]*dynamodb.AttributeValue{
							handlerAndResourceAttr: {
								B: handlerAndResource(rr.key, r),
							},
							resourceVersionAttr: {
								B: n,
							},
						},
					},
				},
			),
		},
	)

	if awsx.IsErrorCode(err, dynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}

	return err == nil, err
}

// handlerAndResource returns an identifier based on the handler and resource
// identifiers.
func handlerAndResource(handler string, r []byte) []byte {
	return []byte(handler + " " + string(r))
}
