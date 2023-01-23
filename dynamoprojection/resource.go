package dynamoprojection

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dogmatiq/projectionkit/resource"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in an AWS DynamoDB database.
type ResourceRepository struct {
	db       *dynamodb.DynamoDB
	key      string
	occTable string
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new DynamoDB resource repository.
func NewResourceRepository(
	db *dynamodb.DynamoDB,
	key, occTable string,
) *ResourceRepository {
	return &ResourceRepository{db, key, occTable}
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	out, err := rr.db.GetItemWithContext(
		ctx,
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

	_, err := rr.db.PutItemWithContext(
		ctx,
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
	_, err := rr.db.DeleteItemWithContext(
		ctx,
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

func (rr *ResourceRepository) createResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	_, err := rr.db.TransactWriteItems(
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

	if isErrorCode(err, dynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}

	return err == nil, err
}

func (rr *ResourceRepository) deleteResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	// TO-DO: add a version check prior to deletion.
	// Also add a test for that.
	_, err := rr.db.TransactWriteItems(
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				items,
				&dynamodb.TransactWriteItem{
					Delete: &dynamodb.Delete{
						TableName: aws.String(rr.occTable),
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

	return err == nil, err
}

func (rr *ResourceRepository) updateResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...*dynamodb.TransactWriteItem,
) (bool, error) {
	_, err := rr.db.TransactWriteItems(
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

	if isErrorCode(err, dynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}

	return err == nil, err
}

// handlerAndResource returns an identifier based on the handler and resource
// identifiers.
func handlerAndResource(handler string, r []byte) []byte {
	return []byte(handler + " " + string(r))
}
