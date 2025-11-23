package dynamoprojection

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/dynamox"
)

var (
	// keyAttr is the name of the attribute that stores the binary
	// representation of the handler's identity key on each item. Together with
	// [streamIDAttr], it forms the primary key of the table.
	handlerKeyAttr = "H"

	// streamIDAttr is the name of the attribute that stores the binary
	// representation of the stream ID on each item.
	streamIDAttr = "S"

	// offsetAttr is the name of the attribute that stores the checkpoint offset
	// on each item.
	offsetAttr = "O"
)

type requests struct {
	Attr struct {
		StreamID   [16]byte                    // [streamIDAttr]
		PrevOffset types.AttributeValueMemberN // [checkpointOffsetAttr]
		NextOffset types.AttributeValueMemberN // [checkpointOffsetAttr]
	}

	Transaction  dynamodb.TransactWriteItemsInput
	PutOffset    types.TransactWriteItem
	UpdateOffset types.TransactWriteItem

	GetOffset  dynamodb.GetItemInput
	GetOffsets dynamodb.QueryInput
}

func (a *adaptor) createTable(ctx context.Context) error {
	return dynamox.CreateTableIfNotExists(
		ctx,
		a.Client,
		a.Table,
		a.OnRequest,
		dynamox.KeyAttr{
			Name:    &handlerKeyAttr,
			Type:    types.ScalarAttributeTypeB,
			KeyType: types.KeyTypeHash,
		},
		dynamox.KeyAttr{
			Name:    &streamIDAttr,
			Type:    types.ScalarAttributeTypeB,
			KeyType: types.KeyTypeRange,
		},
	)
}

// acquireRequests retrieves a prepared [requests] struct from the pool.
func (a *adaptor) acquireRequests() *requests {
	return a.requests.Get().(*requests)
}

// releaseRequests returns a used [requests] struct to the pool.
func (a *adaptor) releaseRequests(r *requests) {
	r.Transaction = dynamodb.TransactWriteItemsInput{}
	a.requests.Put(r)
}

// prepareInputs prepares the DynamoDB API requests used by the adaptor.
//
// The requests are built and reused for the via a [sync.Pool] to avoid repeated
// heap allocations of the same data.
func (a *adaptor) prepareRequests() any {
	var req requests

	req.PutOffset = types.TransactWriteItem{
		Put: &types.Put{
			TableName: &a.Table,
			ExpressionAttributeNames: map[string]string{
				"#H": handlerKeyAttr,
			},
			Item: map[string]types.AttributeValue{
				handlerKeyAttr: &a.handlerKeyAttr,
				streamIDAttr:   &types.AttributeValueMemberB{Value: req.Attr.StreamID[:]},
				offsetAttr:     &req.Attr.NextOffset,
			},

			// Fail if the record exists so we can detect an OCC conflict.
			ConditionExpression: aws.String(`attribute_not_exists(#H)`),
		},
	}

	req.UpdateOffset = types.TransactWriteItem{
		Update: &types.Update{
			TableName: &a.Table,
			Key: map[string]types.AttributeValue{
				handlerKeyAttr: &a.handlerKeyAttr,
				streamIDAttr:   &types.AttributeValueMemberB{Value: req.Attr.StreamID[:]},
			},
			ExpressionAttributeNames: map[string]string{
				"#O": offsetAttr,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":P": &req.Attr.PrevOffset,
				":N": &req.Attr.NextOffset,
			},
			UpdateExpression: aws.String(`SET #O = :N`),

			// Fail if the record does not exist, or exists with a different
			// checkpoint offset, so we can detect an OCC conflict.
			ConditionExpression: aws.String(`attribute_exists(#O) AND #O = :P`),
		},
	}

	req.GetOffset = dynamodb.GetItemInput{
		TableName: &a.Table,
		Key: map[string]types.AttributeValue{
			handlerKeyAttr: &a.handlerKeyAttr,
			streamIDAttr:   &types.AttributeValueMemberB{Value: req.Attr.StreamID[:]},
		},
	}

	req.GetOffsets = dynamodb.QueryInput{
		TableName:              &a.Table,
		KeyConditionExpression: aws.String("#H = :H"),
		ExpressionAttributeNames: map[string]string{
			"#H": handlerKeyAttr,
			"#O": offsetAttr,
			"#S": streamIDAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":H": &a.handlerKeyAttr,
		},
		ProjectionExpression: aws.String("#S, #O"),
	}

	return &req
}

func (a *adaptor) makeDeleteOperation(item map[string]types.AttributeValue) types.TransactWriteItem {
	return types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: &a.Table,
			Key: map[string]types.AttributeValue{
				handlerKeyAttr: &a.handlerKeyAttr,
				streamIDAttr:   item[streamIDAttr],
			},
			ExpressionAttributeNames: map[string]string{
				"#O": offsetAttr,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":P": item[offsetAttr],
			},

			// Fail if the record does not exist, or exists with a different
			// checkpoint offset, so we can detect an OCC conflict.
			ConditionExpression: aws.String(`attribute_exists(#O) AND #O = :P`),
		},
	}
}

func (a *adaptor) marshalOffset(cp uint64) string {
	return strconv.FormatUint(cp, 10)
}

func (a *adaptor) unmarshalOffset(item map[string]types.AttributeValue) (uint64, error) {
	s, ok := item[offsetAttr]
	if !ok {
		return 0, fmt.Errorf(
			"%q table is missing %q attribute",
			a.Table,
			offsetAttr,
		)
	}

	n, ok := s.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf(
			"%q table is has invalid %q attribute: expected number type, got %T",
			a.Table,
			offsetAttr,
			s,
		)
	}

	cp, err := strconv.ParseUint(n.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"%q table has invalid %q attribute: %w",
			a.Table,
			offsetAttr,
			err,
		)
	}

	return cp, nil
}
