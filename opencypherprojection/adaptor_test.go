package neo4jprojection_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/opencypherprojection"
	"github.com/dogmatiq/projectionkit/opencypherprojection/fixtures" // can't dot-import due to conflict
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {

	var (
		ctx     context.Context
		cancel  context.CancelFunc
		handler *fixtures.MessageHandler
		db      neo4j.DriverWithContext
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		var err error
		db, err = neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "dogmamgod", ""))
		// hostAndPort := "nomad-cluster-neptune.cluster-cojplssy37iq.ap-southeast-2.neptune.amazonaws.com:8182"
		// db, err = neo4j.NewDriverWithContext("bolt+ssc://"+hostAndPort+"/opencypher", neo4j.NoAuth())
		Expect(err).ShouldNot(HaveOccurred())

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		adaptor = New(db, handler, "test:projection_occ")
	})

	AfterEach(func() {

		session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

		_, err := session.Run(ctx,
			`MATCH (n:test:projection_occ) DETACH DELETE n`,
			map[string]any{},
		)
		Expect(err).ShouldNot(HaveOccurred())

		session.Close(ctx)
		db.Close(ctx)
		cancel()
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

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
				neo4j.ExplicitTransaction,
				dogma.ProjectionEventScope,
				dogma.Message,
			) error {
				return terr
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

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d neo4j.DriverWithContext,
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
