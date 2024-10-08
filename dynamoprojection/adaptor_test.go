package dynamoprojection_test

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	"github.com/dogmatiq/projectionkit/dynamoprojection/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {
	var (
		ctx     context.Context
		handler *fixtures.MessageHandler
		client  *dynamodb.Client
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

		endpoint := os.Getenv("DOGMATIQ_TEST_DYNAMODB_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:28000"
		}

		cfg, err := config.LoadDefaultConfig(
			ctx,
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(
				aws.EndpointResolverWithOptionsFunc(
					func(
						service, region string,
						options ...interface{},
					) (aws.Endpoint, error) {
						return aws.Endpoint{
							PartitionID: "aws",
							URL:         endpoint,
						}, nil
					},
				),
			),
			config.WithCredentialsProvider(
				credentials.StaticCredentialsProvider{
					Value: aws.Credentials{
						AccessKeyID:     "id",
						SecretAccessKey: "secret",
						SessionToken:    "",
					},
				},
			),
			config.WithRetryer(
				func() aws.Retryer {
					return aws.NopRetryer{}
				},
			),
		)
		Expect(err).ShouldNot(HaveOccurred())

		client = dynamodb.NewFromConfig(cfg)

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		err = CreateTable(ctx, client, "ProjectionOCCTable")
		Expect(err).ShouldNot(HaveOccurred())

		err = dynamodb.NewTableExistsWaiter(client).Wait(
			ctx,
			&dynamodb.DescribeTableInput{
				TableName: aws.String("ProjectionOCCTable"),
			},
			5*time.Second,
		)
		Expect(err).ShouldNot(HaveOccurred())

		adaptor = New(client, "ProjectionOCCTable", handler)
	})

	AfterEach(func() {
		err := DeleteTable(ctx, client, "ProjectionOCCTable")
		Expect(err).ShouldNot(HaveOccurred())

		err = dynamodb.NewTableNotExistsWaiter(client).Wait(
			ctx,
			&dynamodb.DescribeTableInput{
				TableName: aws.String("ProjectionOCCTable"),
			},
			5*time.Second,
		)
		Expect(err).ShouldNot(HaveOccurred())
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func New()", func() {
		It("returns an unbound handler if the client is nil", func() {
			adaptor = New(nil, "ProjectionOCCTable", handler)

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("projection handler has not been bound to a database"))
		})
	})

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(adaptor)).To(Equal("<key>"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("returns an error if the application's message handler fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventFunc = func(
				context.Context,
				dogma.ProjectionEventScope,
				dogma.Event,
			) ([]types.TransactWriteItem, error) {
				return nil, terr
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				EventA1,
			)
			Expect(err).Should(HaveOccurred())
		})

		When("transaction items returned by a user cause conflict", func() {
			BeforeEach(func() {
				_, err := client.CreateTable(
					ctx,
					&dynamodb.CreateTableInput{
						TableName: aws.String("TestTable"),
						AttributeDefinitions: []types.AttributeDefinition{
							{
								AttributeName: aws.String("PK"),
								AttributeType: types.ScalarAttributeTypeS,
							},
						},
						KeySchema: []types.KeySchemaElement{
							{
								AttributeName: aws.String("PK"),
								KeyType:       types.KeyTypeHash,
							},
						},
						BillingMode: types.BillingModePayPerRequest,
					},
				)
				if !errors.As(err, new(*types.ResourceInUseException)) {
					Expect(err).ShouldNot(HaveOccurred())
				}

				err = dynamodb.NewTableExistsWaiter(client).Wait(
					ctx,
					&dynamodb.DescribeTableInput{
						TableName: aws.String("TestTable"),
					},
					5*time.Second,
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			AfterEach(func() {
				_, err := client.DeleteTable(
					ctx,
					&dynamodb.DeleteTableInput{
						TableName: aws.String("TestTable"),
					},
				)
				Expect(err).ShouldNot(HaveOccurred())

				err = dynamodb.NewTableNotExistsWaiter(client).Wait(
					ctx,
					&dynamodb.DescribeTableInput{
						TableName: aws.String("TestTable"),
					},
					5*time.Second,
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("returns an error", func() {
				handler.HandleEventFunc = func(
					context.Context,
					dogma.ProjectionEventScope,
					dogma.Event,
				) ([]types.TransactWriteItem, error) {
					return []types.TransactWriteItem{
						{
							ConditionCheck: &types.ConditionCheck{
								TableName: aws.String("TestTable"),
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

				_, err := adaptor.HandleEvent(
					context.Background(),
					[]byte("<resource>"),
					nil,
					[]byte("<version 01>"),
					nil,
					EventA1,
				)
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.TransactionCanceledException))).To(BeTrue())
			})
		})
	})

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				c *dynamodb.Client,
				_ dogma.ProjectionCompactScope,
			) error {
				Expect(c).To(BeIdenticalTo(client))
				return errors.New("<error>")
			}

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
