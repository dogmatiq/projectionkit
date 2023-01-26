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

var _ = Context("creating and deleting a table", func() {
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

	Describe("func CreateTable()", func() {
		It("can be called when the table already exists", func() {
			err := CreateTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func DeleteTable()", func() {
		It("can be called when the table does not exist", func() {
			err := DeleteTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("can be called when the table has already been deleted", func() {
			err := CreateTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())

			err = DeleteTable(ctx, db, "ProjectionOCCTable")
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
