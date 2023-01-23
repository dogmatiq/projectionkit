package dynamoprojection

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

// CreateSchema creates an AWS DynamoDB table to store projections on the given
// database.
//
// It does not return an error if the table already exists.
func CreateSchema(
	ctx context.Context,
	db *dynamodb.DynamoDB,
	occTable string,
	decorators ...func(*dynamodb.CreateTableInput) request.Option,
) error {
	in := &dynamodb.CreateTableInput{
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
	}

	var opts []request.Option

	for _, dec := range decorators {
		opts = append(opts, dec(in))
	}

	_, err := db.CreateTableWithContext(ctx, in, opts...)

	if isErrorCode(err, dynamodb.ErrCodeResourceInUseException) {
		return nil
	}

	return err
}

// DropSchema deletes an AWS DynamoDB table that stores data related to
// projection OCC in the given database.
//
// It does not return an error if the table does not exist.
func DropSchema(
	ctx context.Context,
	db *dynamodb.DynamoDB,
	occTable string,
	decorators ...func(*dynamodb.DeleteTableInput) request.Option,
) error {
	in := &dynamodb.DeleteTableInput{
		TableName: aws.String(occTable),
	}

	var opts []request.Option

	for _, dec := range decorators {
		opts = append(opts, dec(in))
	}

	_, err := db.DeleteTableWithContext(ctx, in, opts...)

	if isErrorCode(err, dynamodb.ErrCodeResourceNotFoundException) {
		return nil
	}

	return err
}
