package dynamox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/projectionkit/internal/awsx"
)

// KeyAttr describes a key attribute of a DynamoDB table.
type KeyAttr struct {
	Name    *string
	Type    types.ScalarAttributeType
	KeyType types.KeyType
}

// CreateTableIfNotExists creates a DynamoDB table if it does not exist.
func CreateTableIfNotExists(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
	key ...KeyAttr,
) error {
	req := &dynamodb.CreateTableInput{
		TableName:   &table,
		BillingMode: types.BillingModePayPerRequest,
	}

	for _, k := range key {
		req.AttributeDefinitions = append(
			req.AttributeDefinitions,
			types.AttributeDefinition{
				AttributeName: k.Name,
				AttributeType: k.Type,
			},
		)

		req.KeySchema = append(
			req.KeySchema,
			types.KeySchemaElement{
				AttributeName: k.Name,
				KeyType:       k.KeyType,
			},
		)
	}

	if _, err := awsx.Do(
		ctx,
		client.CreateTable,
		onRequest,
		req,
	); err != nil {
		if errors.As(err, new(*types.ResourceInUseException)) {
			return nil
		}
		return fmt.Errorf("unable to create DynamoDB table: %w", err)
	}

	in := &dynamodb.DescribeTableInput{
		TableName: &table,
	}

	var options []func(*dynamodb.Options)
	if onRequest != nil {
		options = onRequest(in)
	}

	w := dynamodb.NewTableExistsWaiter(
		client,
		func(opts *dynamodb.TableExistsWaiterOptions) {
			opts.ClientOptions = options
		},
	)

	// We set the maximum wait time quite high, as the deadline from ctx, if
	// shorter, will take precedence.
	return w.Wait(ctx, in, 1*time.Minute)
}

// DeleteTableIfExists deletes a DynamoDB table if it exists.
func DeleteTableIfExists(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
) error {
	if _, err := awsx.Do(
		ctx,
		client.DeleteTable,
		onRequest,
		&dynamodb.DeleteTableInput{
			TableName: &table,
		},
	); err != nil {
		if errors.As(err, new(*types.ResourceNotFoundException)) {
			return nil
		}

		return fmt.Errorf("unable to delete DynamoDB table: %w", err)
	}

	return nil
}
