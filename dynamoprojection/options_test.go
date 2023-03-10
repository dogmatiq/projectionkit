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
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	"github.com/dogmatiq/projectionkit/dynamoprojection/fixtures"
	"github.com/dogmatiq/projectionkit/internal/identity"
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
						AccessKeyID:     "<id>",
						SecretAccessKey: "<secret>",
						SessionToken:    "",
					},
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
				"ProjectionOCCTable",
				WithDecorateCreateTable(func(
					in *dynamodb.CreateTableInput,
				) []func(*dynamodb.Options) {
					in.TableName = aws.String("ProjectionOCCTableAlternativeName")
					return nil
				}),
			)
			Expect(err).ShouldNot(HaveOccurred())

			defer func() {
				err := DeleteTable(
					ctx,
					client,
					"ProjectionOCCTableAlternativeName",
				)
				Expect(err).ShouldNot(HaveOccurred())

				err = dynamodb.NewTableNotExistsWaiter(client).Wait(
					ctx,
					&dynamodb.DescribeTableInput{
						TableName: aws.String("ProjectionOCCTable"),
					},
					5*time.Second,
				)
				Expect(err).ShouldNot(HaveOccurred())
			}()

			err = dynamodb.NewTableExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionOCCTableAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can modify the operation via returned options", func() {
			err := CreateTable(
				ctx,
				client,
				"ProjectionOCCTable",
				WithDecorateCreateTable(func(
					*dynamodb.CreateTableInput,
				) []func(*dynamodb.Options) {
					return []func(opts *dynamodb.Options){
						func(opts *dynamodb.Options) {
							opts.Retryer = aws.NopRetryer{}
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
				"ProjectionOCCTableAlternativeName",
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionOCCTableAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(
				ctx,
				client,
				"ProjectionOCCTable",
				WithDecorateDeleteTable(func(
					in *dynamodb.DeleteTableInput,
				) []func(*dynamodb.Options) {
					in.TableName = aws.String("ProjectionOCCTableAlternativeName")
					return nil
				}),
			)
			Expect(err).ShouldNot(HaveOccurred())

			err = dynamodb.NewTableNotExistsWaiter(client).Wait(
				ctx,
				&dynamodb.DescribeTableInput{
					TableName: aws.String("ProjectionOCCTableAlternativeName"),
				},
				5*time.Second,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can modify the operation via returned options", func() {
			err := DeleteTable(
				ctx,
				client,
				"ProjectionOCCTable",
				WithDecorateDeleteTable(func(
					*dynamodb.DeleteTableInput,
				) []func(*dynamodb.Options) {
					return []func(opts *dynamodb.Options){
						func(opts *dynamodb.Options) {
							opts.Retryer = aws.NopRetryer{}
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

	Describe("resource repository options", func() {
		handler := &fixtures.MessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", "<key>")
			},
		}

		Describe("WithDecorateGetItem() option", func() {
			It("can modify the input of the operation", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecorateGetItem(
						func(in *dynamodb.GetItemInput) []func(*dynamodb.Options) {
							in.TableName = aws.String("NonExistingTable")
							return nil
						},
					),
				)

				_, err := repository.ResourceVersion(ctx, []byte("<resource>"))
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecorateGetItem(
						func(*dynamodb.GetItemInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.Retryer = aws.NopRetryer{}
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				_, err := repository.ResourceVersion(ctx, []byte("<resource>"))
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})

		Describe("WithDecoratePutItem() option", func() {
			It("can modify the input of the operation", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecoratePutItem(
						func(in *dynamodb.PutItemInput) []func(*dynamodb.Options) {
							in.TableName = aws.String("NonExistingTable")
							return nil
						},
					),
				)

				err := repository.StoreResourceVersion(
					ctx,
					[]byte("<resource>"),
					[]byte("<version 01>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecoratePutItem(
						func(*dynamodb.PutItemInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.Retryer = aws.NopRetryer{}
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				err := repository.StoreResourceVersion(
					ctx,
					[]byte("<resource>"),
					[]byte("<version 01>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})

		Describe("WithDecorateDeleteItem() option", func() {
			It("can modify the input of the operation", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecorateDeleteItem(
						func(in *dynamodb.DeleteItemInput) []func(*dynamodb.Options) {
							in.TableName = aws.String("NonExistingTable")
							return nil
						},
					),
				)

				err := repository.DeleteResource(
					ctx,
					[]byte("<resource>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecorateDeleteItem(
						func(*dynamodb.DeleteItemInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.Retryer = aws.NopRetryer{}
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				err := repository.DeleteResource(
					ctx,
					[]byte("<resource>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})

		Describe("WithDecorateTransactWriteItems() option", func() {
			It("can modify the input of the operation", func() {
				err := CreateTable(
					ctx,
					client,
					"ProjectionOCCTable",
				)
				Expect(err).ShouldNot(HaveOccurred())

				err = dynamodb.NewTableExistsWaiter(client).Wait(
					ctx,
					&dynamodb.DescribeTableInput{
						TableName: aws.String("ProjectionOCCTable"),
					},
					5*time.Second,
				)
				Expect(err).ShouldNot(HaveOccurred())

				defer func() {
					err := DeleteTable(
						ctx,
						client,
						"ProjectionOCCTable",
					)
					Expect(err).ShouldNot(HaveOccurred())

					err = dynamodb.NewTableNotExistsWaiter(client).Wait(
						ctx,
						&dynamodb.DescribeTableInput{
							TableName: aws.String("ProjectionOCCTable"),
						},
						5*time.Second,
					)
					Expect(err).ShouldNot(HaveOccurred())
				}()

				repository := NewResourceRepository(
					client,
					identity.Key(handler),
					"ProjectionOCCTable",
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

				_, err = repository.UpdateResourceVersion(
					ctx,
					[]byte("<resource>"),
					nil,
					[]byte("<version 01>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(errors.As(err, new(*types.ResourceNotFoundException))).To(BeTrue())
			})

			It("can modify the operation via returned options", func() {
				repository := NewResourceRepository(
					client,
					"ProjectionOCCTable",
					identity.Key(handler),
					WithDecorateTransactWriteItems(
						func(gii *dynamodb.TransactWriteItemsInput) []func(*dynamodb.Options) {
							return []func(opts *dynamodb.Options){
								func(opts *dynamodb.Options) {
									opts.Retryer = aws.NopRetryer{}
									opts.EndpointResolver = dynamodb.EndpointResolverFromURL(
										"http://non-existing-host.com:8000",
									)
								},
							}
						},
					),
				)

				_, err := repository.UpdateResourceVersion(
					ctx,
					[]byte("<resource>"),
					nil,
					[]byte("<version 01>"),
				)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no such host"))
			})
		})
	})
})
