package dynamoprojection_test

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	. "github.com/dogmatiq/projectionkit/dynamoprojection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("creating and dropping schema", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		db     *dynamodb.DynamoDB
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		endpoint := os.Getenv("DOGMATIQ_TEST_DYNAMODB_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:28000"
		}

		config := &aws.Config{
			Credentials: credentials.NewStaticCredentials("<id>", "<secret>", ""),
			Endpoint:    aws.String(endpoint),
			Region:      aws.String("us-east-1"),
			DisableSSL:  aws.Bool(true),
		}

		sess, err := session.NewSession(config)
		Expect(err).ShouldNot(HaveOccurred())

		db = dynamodb.New(sess)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func CreateSchema()", func() {
		It("can be called when the schema already exists", func() {
			err := CreateSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func DropSchema()", func() {
		It("can be called when the schema does not exist", func() {
			err := DropSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can be called when the schema has already been dropped", func() {
			err := CreateSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DropSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DropSchema(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
