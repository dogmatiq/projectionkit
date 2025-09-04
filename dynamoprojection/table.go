package dynamoprojection

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/awsx"
)

// CreateTable creates an AWS DynamoDB table that stores information about
// projection resource versions.
//
// Each running Dogma instance SHOULD use a different table.
// It does not return an error if the table already exists.
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
func CreateTable(
	ctx context.Context,
	c *dynamodb.Client,
	name string,
	options ...TableOption,
) error {
	decorators := &decorators{}
	for _, opt := range options {
		opt.applyTableOption(decorators)
	}

	_, err := awsx.Do(
		ctx,
		c.CreateTable,
		decorators.decorateCreateTableItem,
		&dynamodb.CreateTableInput{
			TableName: aws.String(name),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(keyAttr),
					AttributeType: types.ScalarAttributeTypeB,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(keyAttr),
					KeyType:       types.KeyTypeHash,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		},
	)

	if errors.As(err, new(*types.ResourceInUseException)) {
		return nil
	}

	return err
}

// DeleteTable deletes an AWS DynamoDB table.
//
// It is used to delete tables created using CreateTable().
//
// It does not return an error if the table does not exist.
func DeleteTable(
	ctx context.Context,
	c *dynamodb.Client,
	name string,
	options ...TableOption,
) error {
	decorators := &decorators{}
	for _, opt := range options {
		opt.applyTableOption(decorators)
	}

	_, err := awsx.Do(
		ctx,
		c.DeleteTable,
		decorators.decorateDeleteTableItem,
		&dynamodb.DeleteTableInput{
			TableName: aws.String(name),
		},
	)

	if errors.As(err, new(*types.ResourceNotFoundException)) {
		return nil
	}

	return err
}

const (
	// keyAttr is the name of the attribute that is the key of the projection
	// OCC table. It is derived from the handler key and stream ID.
	keyAttr = "Key"

	// offsetAttr is the name of the checkpoint offset attribute in each item
	// inside the DynamoDB projection OCC table.
	offsetAttr = "Offset"
)

func buildKeyAttr(handler, streamID string) *types.AttributeValueMemberB {
	return &types.AttributeValueMemberB{
		Value: []byte(handler + " " + streamID),
	}
}

func buildOffsetAttr(cp uint64) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{
		Value: strconv.FormatUint(cp, 10),
	}
}
