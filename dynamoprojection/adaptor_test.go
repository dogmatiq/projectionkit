package dynamoprojection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/dynamox"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/internal/handlertest"
	"github.com/testcontainers/testcontainers-go"
	dynamotc "github.com/testcontainers/testcontainers-go/modules/dynamodb"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestAdaptor(t *testing.T) {
	container, err := dynamotc.Run(
		t.Context(),
		"amazon/dynamodb-local",
		dynamotc.WithDisableTelemetry(),
		testcontainers.WithWaitStrategy(
			wait.
				ForHTTP("/").
				WithPort("8000").
				WithStatusCodeMatcher(func(int) bool {
					// Accept any status, we just want to know when it's up.
					return true
				}),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Log(err)
		}
	})

	endpoint, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("id", "secret", ""),
		),
		config.WithRetryer(
			func() aws.Retryer {
				return aws.NopRetryer{}
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := dynamodb.NewFromConfig(
		cfg,
		func(opts *dynamodb.Options) {
			opts.BaseEndpoint = aws.String("http://" + endpoint)
		},
	)

	setup := func(t *testing.T) (deps struct {
		Handler *fixtures.MessageHandler
		Adaptor dogma.ProjectionMessageHandler
	}) {
		t.Helper()

		deps.Handler = &fixtures.MessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", handlertest.IdentityKey)
			},
		}

		deps.Adaptor = New(
			client,
			"ProjectionCheckpoint-"+uuidpb.Generate().AsString(),
			deps.Handler,
		)

		return deps
	}

	handlertest.Run(
		t,
		func(t *testing.T) dogma.ProjectionMessageHandler {
			return setup(t).Adaptor
		},
	)

	t.Run("func HandleEvent()", func(t *testing.T) {
		t.Run("it forwards to the handler", func(t *testing.T) {
			deps := setup(t)
			want := errors.New("<error>")

			deps.Handler.HandleEventFunc = func(
				ctx context.Context,
				s dogma.ProjectionEventScope,
				m dogma.Event,
			) ([]types.TransactWriteItem, error) {
				return nil, want
			}

			_, got := deps.Adaptor.HandleEvent(
				t.Context(),
				&ProjectionEventScopeStub{},
				EventA1,
			)

			if got != want {
				t.Fatalf("unexpected error: got %v, want %v", got, want)
			}
		})
	})

	t.Run("when transaction items returned by the handler cause conflict", func(t *testing.T) {
		t.Run("it returns an error", func(t *testing.T) {
			deps := setup(t)

			table := "TestTable-" + uuidpb.Generate().AsString()

			if err := dynamox.CreateTableIfNotExists(
				t.Context(),
				client,
				table,
				nil,
				dynamox.KeyAttr{
					Name:    aws.String("PK"),
					Type:    types.ScalarAttributeTypeS,
					KeyType: types.KeyTypeHash,
				},
			); err != nil {
				t.Fatal(err)
			}

			deps.Handler.HandleEventFunc = func(
				ctx context.Context,
				s dogma.ProjectionEventScope,
				m dogma.Event,
			) ([]types.TransactWriteItem, error) {
				return []types.TransactWriteItem{
					{
						ConditionCheck: &types.ConditionCheck{
							TableName: aws.String(table),
							Key: map[string]types.AttributeValue{
								"PK": &types.AttributeValueMemberS{
									Value: "<value>",
								},
							},
							ConditionExpression: aws.String(
								"attribute_exists(PK)",
							),
						},
					},
				}, nil
			}

			_, err := deps.Adaptor.HandleEvent(
				t.Context(),
				&ProjectionEventScopeStub{},
				EventA1,
			)

			if err == nil {
				t.Fatal("expected an error")
			}

			var ex *types.TransactionCanceledException
			if !errors.As(err, &ex) {
				t.Fatalf("unexpected error: got %T(%s), want %T", err, err, ex)
			}
		})

		t.Run("func Compact()", func(t *testing.T) {
			t.Run("it forwards to the handler", func(t *testing.T) {
				deps := setup(t)
				want := errors.New("<error>")

				deps.Handler.CompactFunc = func(
					ctx context.Context,
					c *dynamodb.Client,
					s dogma.ProjectionCompactScope,
				) error {
					if c != client {
						t.Fatalf("unexpected client: got %p, want %p", c, client)
					}
					return want
				}

				got := deps.Adaptor.Compact(
					t.Context(),
					&ProjectionCompactScopeStub{},
				)

				if got != want {
					t.Fatalf("unexpected error: got %v, want %v", got, want)
				}
			})
		})
	})
}
