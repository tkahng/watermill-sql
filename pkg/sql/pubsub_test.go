package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

func newPubSub(t *testing.T, beginner sql.Beginner, consumerGroup string, schemaAdapter sql.SchemaAdapter, offsetsAdapter sql.OffsetsAdapter) (message.Publisher, message.Subscriber) {
	publisher, err := sql.NewPublisher(
		beginner,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		beginner,
		sql.SubscriberConfig{
			ConsumerGroup: consumerGroup,

			PollInterval:   1 * time.Millisecond,
			ResendInterval: 5 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
			OffsetsAdapter: offsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func newPostgreSQL(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	db, err := stdSQL.Open("postgres", connStr)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return sql.BeginnerFromStdSQL(db)
}

func newPgxPostgreSQL(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	conf, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)

	db := stdlib.OpenDB(*conf)

	err = db.Ping()
	require.NoError(t, err)

	return sql.BeginnerFromStdSQL(db)
}

func newPgx(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	conf, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)

	db, err := pgxpool.NewWithConfig(context.Background(), conf)
	require.NoError(t, err)

	err = db.Ping(context.Background())
	require.NoError(t, err)

	return sql.BeginnerFromPgx(db)
}

func createPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		newPostgreSQL(t),
		consumerGroup,
		newPostgresSchemaAdapter(0),
		newPostgresOffsetsAdapter(),
	)
}

func newPostgresOffsetsAdapter() sql.DefaultPostgreSQLOffsetsAdapter {
	return sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}
}

func newPostgresSchemaAdapter(batchSize int) *sql.DefaultPostgreSQLSchema {
	return &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
		SubscribeBatchSize: batchSize,
	}
}

func createPgxPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPgxPostgreSQL(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPgxPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_pgx_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_pgx_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPgx(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPubSubWithConsumerGroup(t, "test")
}

func createPostgreSQLQueue(t *testing.T, db sql.Beginner) (message.Publisher, message.Subscriber) {
	schemaAdapter := sql.PostgreSQLQueueSchema{
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
	}
	offsetsAdapter := sql.PostgreSQLQueueOffsetsAdapter{
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
	}

	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			PollInterval:   1 * time.Millisecond,
			ResendInterval: 5 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
			OffsetsAdapter: offsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func TestPostgreSQLPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPostgreSQLPubSub,
		createPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestPgxPostgreSQLPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPgxPostgreSQLPubSub,
		createPgxPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestPgxPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPgxPubSub,
		createPgxPubSubWithConsumerGroup,
	)
}

func TestPostgreSQLQueue(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      false,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return createPostgreSQLQueue(t, newPostgreSQL(t))
		},
		nil,
	)
}

func TestPgxPostgreSQLQueue(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      false,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return createPostgreSQLQueue(t, newPgxPostgreSQL(t))
		},
		nil,
	)
}

func TestCtxValues(t *testing.T) {
	pubSubConstructors := []struct {
		Name         string
		Constructor  func(t *testing.T) (message.Publisher, message.Subscriber)
		ExpectedType interface{}
	}{

		{
			Name:         "postgresql",
			Constructor:  createPostgreSQLPubSub,
			ExpectedType: &sql.StdSQLTx{},
		},
		{
			Name:         "pgx",
			Constructor:  createPgxPubSub,
			ExpectedType: &sql.PgxTx{},
		},
	}

	for _, constructor := range pubSubConstructors {
		constructor := constructor
		pub, sub := constructor.Constructor(t)

		t.Run(constructor.Name, func(t *testing.T) {
			t.Parallel()
			topicName := "topic_" + watermill.NewUUID()

			err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
			require.NoError(t, err)

			var messagesToPublish []*message.Message

			id := watermill.NewUUID()
			messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

			err = pub.Publish(topicName, messagesToPublish...)
			require.NoError(t, err, "cannot publish message")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			messages, err := sub.Subscribe(ctx, topicName)
			require.NoError(t, err)

			select {
			case msg := <-messages:
				tx, ok := sql.TxFromContext(msg.Context())
				assert.True(t, ok)
				assert.NotNil(t, t, tx)
				assert.IsType(t, constructor.ExpectedType, tx)
				msg.Ack()
			case <-time.After(time.Second * 10):
				t.Fatal("no message received")
			}
		})
	}
}

// TestNotMissingMessages checks if messages are not missing when messages are published in concurrent transactions.
// See more: https://github.com/ThreeDotsLabs/watermill/issues/311
func TestNotMissingMessages(t *testing.T) {
	t.Parallel()

	pubSubs := []struct {
		Name           string
		DbConstructor  func(t *testing.T) sql.Beginner
		SchemaAdapter  sql.SchemaAdapter
		OffsetsAdapter sql.OffsetsAdapter
	}{

		{
			Name:           "postgresql",
			DbConstructor:  newPostgreSQL,
			SchemaAdapter:  newPostgresSchemaAdapter(0),
			OffsetsAdapter: newPostgresOffsetsAdapter(),
		},
		{
			Name: "pgx",
			DbConstructor: func(t *testing.T) sql.Beginner {
				return newPgx(t)
			},
			SchemaAdapter:  newPostgresSchemaAdapter(0),
			OffsetsAdapter: newPostgresOffsetsAdapter(),
		},
	}

	for _, pubSub := range pubSubs {
		pubSub := pubSub

		t.Run(pubSub.Name, func(t *testing.T) {
			t.Parallel()

			db := pubSub.DbConstructor(t)

			topicName := "topic_" + watermill.NewUUID()

			messagesToPublish := []*message.Message{
				message.NewMessage("0", nil),
				message.NewMessage("1", nil),
				message.NewMessage("2", nil),
			}

			sub, err := sql.NewSubscriber(
				db,
				sql.SubscriberConfig{
					ConsumerGroup: "consumerGroup",

					PollInterval:   1 * time.Millisecond,
					ResendInterval: 5 * time.Millisecond,
					SchemaAdapter:  pubSub.SchemaAdapter,
					OffsetsAdapter: pubSub.OffsetsAdapter,
				},
				logger,
			)
			require.NoError(t, err)

			err = sub.SubscribeInitialize(topicName)
			require.NoError(t, err)

			messagesAsserted := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer close(messagesAsserted)

				messages, err := sub.Subscribe(ctx, topicName)
				require.NoError(t, err)

				received, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
				assert.True(t, all)

				tests.AssertAllMessagesReceived(t, messagesToPublish, received)
			}()

			tx0, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx1, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			txRollback, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx2, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			pub0, err := sql.NewPublisher(
				tx0,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub0.Publish(topicName, messagesToPublish[0])
			require.NoError(t, err, "cannot publish message")

			pub1, err := sql.NewPublisher(
				tx1,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub1.Publish(topicName, messagesToPublish[1])
			require.NoError(t, err, "cannot publish message")

			pubRollback, err := sql.NewPublisher(
				txRollback,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pubRollback.Publish(topicName, message.NewMessage("rollback", nil))
			require.NoError(t, err, "cannot publish message")

			pub2, err := sql.NewPublisher(
				tx2,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub2.Publish(topicName, messagesToPublish[2])
			require.NoError(t, err, "cannot publish message")

			require.NoError(t, tx2.Commit())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, txRollback.Rollback())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx1.Commit())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx0.Commit())
			time.Sleep(time.Millisecond * 10)

			<-messagesAsserted
		})
	}
}

func TestConcurrentSubscribe_different_bulk_sizes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
		Test        func(t *testing.T, tCtx tests.TestContext, pubSubConstructor tests.PubSubConstructor)
	}{

		{
			Name: "TestConcurrentSubscribe_postgresql_1",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(1),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestPublishSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_postgresql_5",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(5),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestConcurrentSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_pgx_1",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPgx(t),
					"test",
					newPostgresSchemaAdapter(1),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestPublishSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_pgx_5",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPgx(t),
					"test",
					newPostgresSchemaAdapter(5),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestConcurrentSubscribe,
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			tc.Test(
				t,
				tests.TestContext{
					TestID: tests.NewTestID(),
					Features: tests.Features{
						ConsumerGroups:                      true,
						ExactlyOnceDelivery:                 true,
						GuaranteedOrder:                     true,
						GuaranteedOrderWithSingleSubscriber: true,
						Persistent:                          true,
					},
				},
				tc.Constructor,
			)
		})
	}
}

func TestDefaultPostgreSQLSchema_planner_mis_estimate_regression(t *testing.T) {
	// this test should be not executed in Parallel to not disturb performance measurements

	db := newPostgreSQL(t)

	offsetsAdapter := newPostgresOffsetsAdapter()

	pub, sub := newPubSub(
		t,
		db,
		"test",
		newPostgresSchemaAdapter(1000),
		offsetsAdapter,
	)

	topicName := "topic_" + watermill.NewUUID()

	err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
	require.NoError(t, err)

	messagesCount := 100_000
	if testing.Short() {
		messagesCount = 1_000
	}
	tests.AddSimpleMessagesParallel(t, messagesCount, pub, topicName, 50)

	subscribeCtx, cancelSubscribe := context.WithCancel(context.Background())

	messages, err := sub.Subscribe(subscribeCtx, topicName)
	require.NoError(t, err)

	// we want to consume most of the messages,
	// but not all to catch performance issues with more unacked messages
	messagesToConsume := int(float64(messagesCount) * 0.8)
	_, all := subscriber.BulkRead(messages, messagesToConsume, time.Minute)
	assert.True(t, all)

	cancelSubscribe()
	<-messages // wait for the subscriber to finish

	schemAdapterBatch1 := newPostgresSchemaAdapter(1)
	q, err := schemAdapterBatch1.SelectQuery(sql.SelectQueryParams{
		Topic:          topicName,
		ConsumerGroup:  "",
		OffsetsAdapter: offsetsAdapter,
	})
	require.NoError(t, err)

	var analyseResult string

	res, err := db.QueryContext(context.Background(), "EXPLAIN ANALYZE\n"+q.Query, q.Args...)
	require.NoError(t, err)

	for res.Next() {
		var line string
		err := res.Scan(&line)
		require.NoError(t, err)
		analyseResult += line + "\n"
	}
	require.NoError(t, res.Close())

	t.Log(analyseResult)

	rowsRemovedByFilter := findRowsRemovedByFilterInAnalyze(analyseResult)

	for _, i := range rowsRemovedByFilter {
		assert.LessOrEqual(
			t,
			i,
			1,
			"too many rows removed by filter - it's likely a performance regression",
		)
	}

	duration := extractDurationFromAnalyze(t, analyseResult)

	// TBD if it will be stable in CI
	assert.LessOrEqual(t, duration, time.Millisecond, "query duration is too long")
}

func findRowsRemovedByFilterInAnalyze(input string) []int {
	pattern := `Rows Removed by Filter: (\d+)`
	re := regexp.MustCompile(pattern)

	matches := re.FindAllStringSubmatch(input, -1)

	result := make([]int, 0, len(matches))

	for _, match := range matches {
		if len(match) > 1 {
			value, err := strconv.Atoi(match[1])
			if err == nil {
				result = append(result, value)
			}
		}
	}

	return result
}

func extractDurationFromAnalyze(t *testing.T, s string) time.Duration {
	t.Helper()

	// Regular expression to match "Execution Time: X.XXX ms" pattern
	re := regexp.MustCompile(`Execution Time:\s*(\d+(?:\.\d+)?)\s*ms`)

	match := re.FindStringSubmatch(s)
	if len(match) != 2 {
		t.Fatalf("cannot find duration in the string: %s", s)
	}

	durationStr := match[1]

	parsed, err := time.ParseDuration(durationStr + "ms")
	require.NoError(t, err)

	return parsed
}
