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
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	"github.com/dogmatiq/projectionkit/dynamoprojection/internal/fixtures" // can't dot-import due to conflict
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("adding options", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		client *dynamodb.Client
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		endpoint := os.Getenv("DOGMATIQ_TEST_DYNAMODB_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:28000"
		}

		cfg, err := config.LoadDefaultConfig(
			ctx,
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(
				aws.EndpointResolverWithOptionsFunc(
					func(service, region string, options ...interface{}) (aws.Endpoint, error) {
						return aws.Endpoint{URL: endpoint}, nil
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
	})

	AfterEach(func() {
		cancel()
	})

	Describe("CreateTable() options", func() {
		It("can modify the input of the operation", func() {
			err := CreateTable(
				ctx,
				client,
				"ProjectionCheckpoint",
				WithDecorateCreateTable(func(
					in *dynamodb.CreateTableInput,
				) []func(*dynamodb.Options) {
					in.TableName = aws.String("ProjectionCheckpointAlternativeName")
					return nil
				}),
			)
			Expect(err).ShouldNot(HaveOccurred())

			defer func() {
				err := DeleteTable(
					ctx,
					client,
					"ProjectionCheckpointAlternativeName",
				)
				Expect(err).ShouldNot(HaveOccurred())

				err = dynamodb.NewTableNotExistsWaiter(client).Wait(
					ctx,
					&dynamodb.DescribeTableInput{
						TableName: aws.String("ProjectionCheckpoint"),
					},
					5*time.Second,
				)
				Expect(err).ShouldNot(HaveOccurred())
			}()

			err = dynamodb.NewTableExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionCheckpointAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can modify the operation via returned options", func() {
			err := CreateTable(
				ctx,
				client,
				"ProjectionCheckpoint",
				WithDecorateCreateTable(func(
					*dynamodb.CreateTableInput,
				) []func(*dynamodb.Options) {
					return []func(opts *dynamodb.Options){
						func(opts *dynamodb.Options) {
							opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
								"http://non-existing-host.com:8000",
							)
						},
					}
				}),
			)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no such host"))
		})
	})

	Describe("DeleteTable() options", func() {
		It("can modify the input of the operation", func() {
			err := CreateTable(
				ctx,
				client,
				"ProjectionCheckpointAlternativeName",
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionCheckpointAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(
				ctx,
				client,
				"ProjectionCheckpoint",
				WithDecorateDeleteTable(func(
					in *dynamodb.DeleteTableInput,
				) []func(*dynamodb.Options) {
					in.TableName = aws.String("ProjectionCheckpointAlternativeName")
					return nil
				}),
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableNotExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionCheckpointAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can modify the operation via returned options", func() {
			err := DeleteTable(
				ctx,
				client,
				"ProjectionCheckpoint",
				WithDecorateDeleteTable(func(
					*dynamodb.DeleteTableInput,
				) []func(*dynamodb.Options) {
					return []func(opts *dynamodb.Options){
						func(opts *dynamodb.Options) {
							opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
								"http://non-existing-host.com:8000",
							)
						},
					}
				}),
			)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no such host"))
		})
	})

	Describe("New() options", func() {
		handler := &fixtures.MessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", "35a2926d-3381-41e8-8189-7fb53104b8b5")
			},
		}

		BeforeEach(func() {
			err := CreateTable(
				ctx,
				client,
				"ProjectionCheckpoint",
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionCheckpoint"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			err := DeleteTable(
				ctx,
				client,
				"ProjectionCheckpoint",
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableNotExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionCheckpoint"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		Describe("WithDecorateGetItem() option", func() {
			It("can modify the input of the operation", func() {
				adaptor := New(
					client,
					"ProjectionCheckpoint",
					handler,
					WithDecorateGetItem(
						func(in *dynamodb.GetItemInput) []func(*dynamodb.Options) {
							in.TableName = aws.String("NonExistingTable")
							return nil
						},
					),
				)

				_, err := adaptor.CheckpointOffset(ctx, "67c1c759-fcb8-4344-b8b9-cf456ab31ba5")
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				adaptor := New(
					client,
					"ProjectionCheckpoint",
					handler,
					WithDecorateGetItem(
						func(*dynamodb.GetItemInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				_, err := adaptor.CheckpointOffset(ctx, "67c1c759-fcb8-4344-b8b9-cf456ab31ba5")
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})

		Describe("WithDecorateTransactWriteItems() option", func() {
			It("can modify the input of the operation", func() {
				adaptor := New(
					client,
					"ProjectionCheckpoint",
					handler,
					WithDecorateTransactWriteItems(
						func(in *dynamodb.TransactWriteItemsInput) []func(*dynamodb.Options) {
							in.TransactItems = append(
								in.TransactItems,
								types.TransactWriteItem{
									ConditionCheck: &types.ConditionCheck{
										TableName: aws.String("NonExistingTable"),
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
							)
							return nil
						},
					),
				)

				_, err := adaptor.HandleEvent(
					ctx,
					&stubs.ProjectionEventScopeStub{},
					EventA1,
				)
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				adaptor := New(
					client,
					"ProjectionCheckpoint",
					handler,
					WithDecorateTransactWriteItems(
						func(gii *dynamodb.TransactWriteItemsInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				_, err := adaptor.HandleEvent(
					ctx,
					&stubs.ProjectionEventScopeStub{},
					EventA1,
				)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})
	})
})
