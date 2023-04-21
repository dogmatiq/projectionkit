package dynamoprojection_test

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("creating and deleting a table", func() {
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

	Describe("func CreateTable()", func() {
		It("can be called when the table already exists", func() {
			err := CreateTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func DeleteTable()", func() {
		It("can be called when the table does not exist", func() {
			err := DeleteTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can be called when the table has already been deleted", func() {
			err := CreateTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(ctx, client, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
