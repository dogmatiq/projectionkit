package dynamoprojection

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/dynamox"
	"github.com/dogmatiq/projectionkit/internal/awsx"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/syncx"
)

// adaptor adapts a [ProjectionMessageHandler] to the
// [dogma.ProjectionMessageHandler] interface.
type adaptor struct {
	Client    *dynamodb.Client
	Table     string
	Handler   MessageHandler
	OnRequest func(any) []func(*dynamodb.Options)

	handlerKeyAttr  types.AttributeValueMemberB // [handlerKeyAttr]
	requests        sync.Pool
	createTableOnce syncx.SucceedOnce
}

// New returns a new [dogma.ProjectionMessageHandler] that binds a
// DynamoDB-specific [MessageHandler] to a DynamoDB client.
//
// The handler stores information about the projection's checkpoint offsets in
// the given table. Each application should have its own DynamoDB table.
func New(
	client *dynamodb.Client,
	table string,
	handler MessageHandler,
	options ...Option,
) dogma.ProjectionMessageHandler {
	handlerKey := identity.Key(handler)

	a := &adaptor{
		Client:  client,
		Table:   table,
		Handler: handler,

		handlerKeyAttr: types.AttributeValueMemberB{
			Value: handlerKey[:],
		},
	}

	a.requests.New = a.prepareRequests

	for _, opt := range options {
		opt(a)
	}

	return a
}

// Option is a functional option that changes the behavior of [New].
type Option func(*adaptor)

// WithRequestHook is an [Option] that configures fn as a pre-request hook.
//
// Before each DynamoDB API request, fn is passed a pointer to the input struct,
// e.g. [dynamodb.GetItemInput], which it may modify in-place. It may be called
// with any DynamoDB request type. The types of requests used may change in any
// version without notice.
//
// Any functions returned by fn will be applied to the request's options before
// the request is sent.
func WithRequestHook(fn func(any) []func(*dynamodb.Options)) Option {
	return func(a *adaptor) {
		a.OnRequest = fn
	}
}

func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.Handler.Configure(c)
}

func (a *adaptor) HandleEvent(
	ctx context.Context,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) (uint64, error) {
	if err := a.createTableOnce.Do(ctx, a.createTable); err != nil {
		return 0, err
	}

	req := a.acquireRequests()
	defer a.releaseRequests(req)

	var err error
	req.Transaction.TransactItems, err = a.Handler.HandleEvent(ctx, s, m)
	if err != nil {
		return 0, err
	}

	var (
		prev = s.CheckpointOffset()
		next = s.Offset() + 1
	)

	req.Attr.StreamID = uuidpb.MustParseAsByteArray(s.StreamID())
	req.Attr.NextOffset.Value = a.marshalOffset(next)

	if s.CheckpointOffset() == 0 {
		req.Transaction.TransactItems = append(req.Transaction.TransactItems, req.PutOffset)
	} else {
		req.Attr.PrevOffset.Value = a.marshalOffset(prev)
		req.Transaction.TransactItems = append(req.Transaction.TransactItems, req.UpdateOffset)
	}

	_, err = awsx.Do(
		ctx,
		a.Client.TransactWriteItems,
		a.OnRequest,
		&req.Transaction,
	)

	if isOCCConflict(err) {
		return a.checkpointOffset(ctx, req)
	}

	return next, err
}

func (a *adaptor) CheckpointOffset(ctx context.Context, id string) (uint64, error) {
	req := a.acquireRequests()
	defer a.releaseRequests(req)

	req.Attr.StreamID = uuidpb.MustParseAsByteArray(id)
	return a.checkpointOffset(ctx, req)
}

func (a *adaptor) checkpointOffset(ctx context.Context, req *requests) (uint64, error) {
	out, err := awsx.Do(
		ctx,
		a.Client.GetItem,
		a.OnRequest,
		&req.GetOffset,
	)
	if err != nil {
		if isTableNotFound(err) {
			// If the table used to track offsets does not exist, we can't have
			// handled any events yet, so the checkpoint offset is zero.
			return 0, nil
		}

		return 0, err
	}

	if out.Item == nil {
		return 0, nil
	}

	return a.unmarshalOffset(out.Item)
}

func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.Handler.Compact(ctx, a.Client, s)
}

func (a *adaptor) Reset(ctx context.Context, s dogma.ProjectionResetScope) error {
	req := a.acquireRequests()
	defer a.releaseRequests(req)

	if err := dynamox.QueryRange(
		ctx,
		a.Client,
		a.OnRequest,
		&req.GetOffsets,
		func(
			_ context.Context,
			item map[string]types.AttributeValue,
		) (bool, error) {
			req.Transaction.TransactItems = append(
				req.Transaction.TransactItems,
				a.makeDeleteOperation(item),
			)
			return true, nil
		},
	); err != nil {
		if isTableNotFound(err) {
			// If the table used to track offsets does not exist, there is
			// nothing to reset.
			return nil
		}

		return err
	}

	if len(req.Transaction.TransactItems) == 0 {
		return nil
	}

	items, err := a.Handler.Reset(ctx, s)
	if err != nil {
		return err
	}

	req.Transaction.TransactItems = append(req.Transaction.TransactItems, items...)

	_, err = awsx.Do(
		ctx,
		a.Client.TransactWriteItems,
		a.OnRequest,
		&req.Transaction,
	)

	return err
}

// isOCCConflict determines if the error from a DynamoDB transaction is caused
// by the conflict in checkpoint table.
func isOCCConflict(err error) bool {
	var x *types.TransactionCanceledException

	if !errors.As(err, &x) {
		return false
	}

	index := len(x.CancellationReasons) - 1
	reason := x.CancellationReasons[index]

	return reason.Code != nil &&
		*reason.Code == "ConditionalCheckFailed"
}

// isTableNotFound determines if the error from a DynamoDB operation is caused
// by the table not existing.
func isTableNotFound(err error) bool {
	var x *types.ResourceNotFoundException
	return errors.As(err, &x)
}
