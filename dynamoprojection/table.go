package dynamoprojection

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/awsx"
)

const (
	// handlerAndResourceAttr is the name of the attribute that combines the
	// identifier of the handler and resource in each item inside the DynamoDB
	// projection OCC table.
	handlerAndResourceAttr = "HandlerAndResource"
	// resourceVersionAttr is the name of the resource version attribute in
	// each item inside the DynamoDB projection OCC table.
	resourceVersionAttr = "Version"
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
		client.CreateTable,
		decorators.decorateCreateTableItem,
		&dynamodb.CreateTableInput{
			TableName: aws.String(occTable),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(handlerAndResourceAttr),
					AttributeType: types.ScalarAttributeTypeB,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(handlerAndResourceAttr),
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
		client.DeleteTable,
		decorators.decorateDeleteTableItem,
		&dynamodb.DeleteTableInput{
			TableName: aws.String(occTable),
		},
	)

	if errors.As(err, new(*types.ResourceNotFoundException)) {
		return nil
	}

	return err
}
