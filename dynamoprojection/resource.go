package dynamoprojection

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/awsx"
	"github.com/dogmatiq/projectionkit/resource"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in AWS DynamoDB.
type ResourceRepository struct {
	client     *dynamodb.Client
	key        string
	occTable   string
	decorators *decorators
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new DynamoDB resource repository.
func NewResourceRepository(
	client *dynamodb.Client,
	key, occTable string,
	options ...ResourceRepositoryOption,
) *ResourceRepository {
	r := &ResourceRepository{
		client:     client,
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
		rr.client.GetItem,
		rr.decorators.decorateGetItem,
		&dynamodb.GetItemInput{
			TableName: aws.String(rr.occTable),
			Key: map[string]types.AttributeValue{
				handlerAndResourceAttr: &types.AttributeValueMemberB{
					Value: handlerAndResource(rr.key, r),
				},
			},
		},
	)

	if err != nil || out.Item == nil {
		return nil, err
	}

	b, ok := out.Item[resourceVersionAttr].(*types.AttributeValueMemberB)
	if !ok {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the integrity of the record in the projection OCC table.
		panic(
			fmt.Sprintf(
				"invalid structure in projection OCC table %s",
				rr.occTable,
			),
		)
	}

	return b.Value, nil
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
		rr.client.PutItem,
		rr.decorators.decoratePutItem,
		&dynamodb.PutItemInput{
			TableName: aws.String(rr.occTable),
			Item: map[string]types.AttributeValue{
				handlerAndResourceAttr: &types.AttributeValueMemberB{
					Value: handlerAndResource(rr.key, r),
				},
				resourceVersionAttr: &types.AttributeValueMemberB{
					Value: v,
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
	items ...types.TransactWriteItem,
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
		rr.client.DeleteItem,
		rr.decorators.decorateDeleteItem,
		&dynamodb.DeleteItemInput{
			TableName: aws.String(rr.occTable),
			Key: map[string]types.AttributeValue{
				handlerAndResourceAttr: &types.AttributeValueMemberB{
					Value: handlerAndResource(rr.key, r),
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
	items ...types.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.client.TransactWriteItems,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				[]types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName:           aws.String(rr.occTable),
							ConditionExpression: aws.String(`attribute_not_exists(#HR)`),
							ExpressionAttributeNames: map[string]string{
								"#HR": handlerAndResourceAttr,
							},
							Item: map[string]types.AttributeValue{
								handlerAndResourceAttr: &types.AttributeValueMemberB{
									Value: handlerAndResource(rr.key, r),
								},
								resourceVersionAttr: &types.AttributeValueMemberB{
									Value: n,
								},
							},
						},
					},
				},
				items...,
			),
		},
	)

	if isOCCConflict(err) {
		return false, nil
	}

	return err == nil, err
}

// deleteResourceWithinTx deletes a resource record in the projection OCC table
// and applies the supplied items within a single transaction.
func (rr *ResourceRepository) deleteResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...types.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.client.TransactWriteItems,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				[]types.TransactWriteItem{
					{
						Delete: &types.Delete{
							TableName:           aws.String(rr.occTable),
							ConditionExpression: aws.String(`attribute_exists(#HR) AND #V = :C`),
							ExpressionAttributeNames: map[string]string{
								"#HR": handlerAndResourceAttr,
								"#V":  resourceVersionAttr,
							},
							ExpressionAttributeValues: map[string]types.AttributeValue{
								":C": &types.AttributeValueMemberB{
									Value: c,
								},
							},
							Key: map[string]types.AttributeValue{
								handlerAndResourceAttr: &types.AttributeValueMemberB{
									Value: handlerAndResource(rr.key, r),
								},
							},
						},
					},
				},
				items...,
			),
		},
	)

	if isOCCConflict(err) {
		return false, nil
	}

	return err == nil, err
}

// updateResourceWithinTx updates a resource record in the projection OCC table
// and applies the supplied items within a single transaction.
func (rr *ResourceRepository) updateResourceWithinTx(
	ctx context.Context,
	r, c, n []byte,
	items ...types.TransactWriteItem,
) (bool, error) {
	_, err := awsx.Do(
		ctx,
		rr.client.TransactWriteItems,
		rr.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				[]types.TransactWriteItem{
					{
						Update: &types.Update{
							TableName: aws.String(rr.occTable),
							Key: map[string]types.AttributeValue{
								handlerAndResourceAttr: &types.AttributeValueMemberB{
									Value: handlerAndResource(rr.key, r),
								},
							},
							ConditionExpression: aws.String(`attribute_exists(#HR) AND #V = :C`),
							UpdateExpression:    aws.String(`SET #V = :N`),
							ExpressionAttributeNames: map[string]string{
								"#HR": handlerAndResourceAttr,
								"#V":  resourceVersionAttr,
							},
							ExpressionAttributeValues: map[string]types.AttributeValue{
								":C": &types.AttributeValueMemberB{
									Value: c,
								},
								":N": &types.AttributeValueMemberB{
									Value: n,
								},
							},
						},
					},
				},
				items...,
			),
		},
	)

	if isOCCConflict(err) {
		return false, nil
	}

	return err == nil, err
}

// isOCCConflict determines if the error is caused by the conflict in OCC table
// in the process of transaction handling.
//
// This function heavily relies on the assumption that the transaction item to
// update projection OCC table is the first in the list preceding user-provided
// transaction items.
func isOCCConflict(err error) bool {
	var txCancelErr *types.TransactionCanceledException
	if errors.As(err, &txCancelErr) {
		if *txCancelErr.CancellationReasons[0].Code == "ConditionalCheckFailed" {
			return true
		}
	}

	return false
}

// handlerAndResource returns an identifier based on the handler and resource
// identifiers.
func handlerAndResource(handler string, r []byte) []byte {
	return []byte(handler + " " + string(r))
}
