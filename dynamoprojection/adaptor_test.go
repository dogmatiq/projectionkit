package dynamoprojection_test

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
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
		db      *dynamodb.DynamoDB
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx = context.Background()

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

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		err = CreateSchema(ctx, db, "ProjectionOCCTable")
		Expect(err).ShouldNot(HaveOccurred())

		adaptor = New(db, "ProjectionOCCTable", handler)
	})

	AfterEach(func() {
		err := DropSchema(ctx, db, "ProjectionOCCTable")
		Expect(err).ShouldNot(HaveOccurred())
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func New()", func() {
		It("returns an unbound handler if the database is nil", func() {
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
				dogma.Message,
			) ([]*dynamodb.TransactWriteItem, error) {
				return nil, terr
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				MessageA1,
			)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func TimeoutHint()", func() {
		It("forwards to the handler", func() {
			handler.TimeoutHintFunc = func(
				m dogma.Message,
			) time.Duration {
				Expect(m).To(BeIdenticalTo(MessageA1))
				return 100 * time.Millisecond
			}

			d := adaptor.TimeoutHint(
				MessageA1,
			)
			Expect(d).To(Equal(100 * time.Millisecond))
		})
	})

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d *dynamodb.DynamoDB,
				_ dogma.ProjectionCompactScope,
			) error {
				Expect(d).To(BeIdenticalTo(db))
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
