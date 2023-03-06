package dynamoprojection

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/internal/identity"
	"github.com/dogmatiq/projectionkit/internal/unboundhandler"
	"github.com/dogmatiq/projectionkit/resource"
)

// adaptor adapts a dynamoprojection.ProjectionMessageHandler to the
// dogma.ProjectionMessageHandler interface.
type adaptor struct {
	client  *dynamodb.Client
	handler MessageHandler
	repo    *ResourceRepository
}

// New returns a new Dogma projection message handler by binding a
// DynamoDB-specific projection handler to an AWS DynamoDB database.
//
// occTable is the table that is used for the data related to projection OCC.
// The caller MUST specify the name of the table used for storing the data
// related to projection OCC.
//
// If db is nil the returned handler will return an error whenever an operation
// that requires the database is performed.
func New(
	client *dynamodb.Client,
	occTable string,
	h MessageHandler,
	options ...HandlerOption,
) dogma.ProjectionMessageHandler {
	if client == nil {
		return unboundhandler.New(h)
	}

	var rrOpts []ResourceRepositoryOption
	for _, opt := range options {
		if rrOpt, ok := opt.(ResourceRepositoryOption); ok {
			rrOpts = append(rrOpts, rrOpt)
		}
	}

	a := &adaptor{
		client:  client,
		handler: h,
		repo: NewResourceRepository(
			client,
			identity.Key(h),
			occTable,
			rrOpts...,
		),
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
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	items, err := a.handler.HandleEvent(ctx, s, m)
	if err != nil {
		return false, err
	}

	return a.repo.UpdateResourceVersionAndTransactionItems(ctx, r, c, n, items...)
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return a.repo.ResourceVersion(ctx, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.repo.DeleteResource(ctx, r)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *adaptor) TimeoutHint(m dogma.Message) time.Duration {
	return a.handler.TimeoutHint(m)
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return a.handler.Compact(ctx, a.client, s)
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor) ResourceRepository() resource.Repository {
	return a.repo
}
