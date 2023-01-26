package dynamoprojection

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

// CreateTable creates an AWS DynamoDB table to store projections on the given
// database.
//
// occTable is the name of the table that stores the data related to the
// projection OCC. For AWS DynamoDB naming rules, see [this link](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html)
// for reference.
//
// It does not return an error if the table already exists.
func CreateTable(
	ctx context.Context,
	db *dynamodb.DynamoDB,
	occTable string,
	options ...TableOption,
) error {
	decorators := &decorators{}
	for _, opt := range options {
		opt.applyTableOption(decorators)
	}

	_, err := awsx.Do(
		ctx,
		db.CreateTableWithContext,
		decorators.decorateCreateTableItem,
		&dynamodb.CreateTableInput{
			TableName: aws.String(occTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(handlerAndResourceAttr),
					AttributeType: aws.String("B"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(handlerAndResourceAttr),
					KeyType:       aws.String("HASH"),
				},
			},
			BillingMode: aws.String("PAY_PER_REQUEST"),
		},
	)

	if awsx.IsErrorCode(err, dynamodb.ErrCodeResourceInUseException) {
		return nil
	}

	return err
}

// DeleteTable deletes an AWS DynamoDB table that stores data related to
// projection OCC in the given database.
//
// occTable is the name of the table that stores the data related to the
// projection OCC.
//
// It does not return an error if the table does not exist.
func DeleteTable(
	ctx context.Context,
	db *dynamodb.DynamoDB,
	occTable string,
	options ...TableOption,
) error {
	decorators := &decorators{}
	for _, opt := range options {
		opt.applyTableOption(decorators)
	}

	_, err := awsx.Do(
		ctx,
		db.DeleteTableWithContext,
		decorators.decorateDeleteTableItem,
		&dynamodb.DeleteTableInput{
			TableName: aws.String(occTable),
		},
	)

	if awsx.IsErrorCode(err, dynamodb.ErrCodeResourceNotFoundException) {
		return nil
	}

	return err
}
