package dynamoprojection

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/awsx"
	"github.com/dogmatiq/projectionkit/internal/identity"
)

// adaptor adapts a [ProjectionMessageHandler] to the
// [dogma.ProjectionMessageHandler] interface.
type adaptor struct {
	client     *dynamodb.Client
	key        string
	occTable   string
	decorators decorators
	handler    MessageHandler
}

// New returns a new Dogma projection message handler by binding a
// DynamoDB-specific projection handler to an AWS DynamoDB client.
//
// t is the name of a DynamoDB table that stores information about projection
// resource versions. Each running Dogma instance SHOULD use a different table.
//
// If c is nil the returned handler will return an error whenever a
// DynamoDB API call is made.
func New(
	c *dynamodb.Client,
	t string,
	h MessageHandler,
	options ...HandlerOption,
) dogma.ProjectionMessageHandler {
	a := &adaptor{
		client:   c,
		key:      identity.Key(h),
		occTable: t,
		handler:  h,
	}

	for _, opt := range options {
		opt.applyOptionToAdaptor(&a.decorators)
	}

	return a
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor) HandleEvent(
	ctx context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) (uint64, error) {
	items, err := a.handler.HandleEvent(ctx, s, m)
	if err != nil {
		return 0, err
	}

	var (
		id   = s.StreamID()
		prev = s.CheckpointOffset()
		next = s.Offset() + 1
	)

	var occItem types.TransactWriteItem

	if s.CheckpointOffset() == 0 {
		occItem.Put = &types.Put{
			TableName:           aws.String(a.occTable),
			ConditionExpression: aws.String(`attribute_not_exists(#K)`),
			ExpressionAttributeNames: map[string]string{
				"#K": keyAttr,
			},
			Item: map[string]types.AttributeValue{
				keyAttr:    buildKeyAttr(a.key, id),
				offsetAttr: buildOffsetAttr(next),
			},
		}
	} else {
		occItem.Update = &types.Update{
			TableName: aws.String(a.occTable),
			Key: map[string]types.AttributeValue{
				keyAttr: buildKeyAttr(a.key, id),
			},
			ConditionExpression: aws.String(`attribute_exists(#K) AND #O = :P`),
			UpdateExpression:    aws.String(`SET #O = :N`),
			ExpressionAttributeNames: map[string]string{
				"#K": keyAttr,
				"#O": offsetAttr,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":P": buildOffsetAttr(prev),
				":N": buildOffsetAttr(next),
			},
		}
	}

	_, err = awsx.Do(
		ctx,
		a.client.TransactWriteItems,
		a.decorators.decorateTransactWriteItems,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: append(
				[]types.TransactWriteItem{occItem}, // must be first, see [isOCCConflict].
				items...,
			),
		},
	)

	if isOCCConflict(err) {
		return a.CheckpointOffset(ctx, id)
	}

	return next, err
}

func (a *adaptor) CheckpointOffset(ctx context.Context, id string) (uint64, error) {
	out, err := awsx.Do(
		ctx,
		a.client.GetItem,
		a.decorators.decorateGetItem,
		&dynamodb.GetItemInput{
			TableName: aws.String(a.occTable),
			Key: map[string]types.AttributeValue{
				keyAttr: buildKeyAttr(a.key, id),
			},
		},
	)

	if err != nil || out.Item == nil {
		return 0, err
	}

	n, ok := out.Item[offsetAttr].(*types.AttributeValueMemberN)
	if !ok {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the integrity of the record in the projection OCC table.
		return 0, fmt.Errorf(
			"%q table is missing %q attribute",
			a.occTable,
			offsetAttr,
		)
	}

	cp, err := strconv.ParseUint(n.Value, 10, 64)
	if err != nil {
		// CODE COVERAGE: This branch can not be easily covered without somehow
		// breaking the integrity of the record in the projection OCC table.
		return 0, fmt.Errorf(
			"%q table has invalid %q attribute: %w",
			a.occTable,
			offsetAttr,
			err,
		)
	}

	return cp, nil
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.client, s)
}

// isOCCConflict determines if the error from a DynamoDB transaction is caused
// by the conflict in OCC table.
//
// It assumes that the transaction item to update projection OCC table is the
// first in the list, preceding transaction items created by the application
// handler implementation.
func isOCCConflict(err error) bool {
	var txCancelErr *types.TransactionCanceledException
	if errors.As(err, &txCancelErr) {
		if *txCancelErr.CancellationReasons[0].Code == "ConditionalCheckFailed" {
			return true
		}
	}

	return false
}
