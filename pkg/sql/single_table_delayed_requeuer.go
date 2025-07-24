package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/requeuer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// SingleTableDelayedRequeuer is a requeuer that uses a delayed publisher and subscriber to requeue messages.
//
// After creating it, you should:
// 1. Add the Middleware() to your router.
// 2. Run it with the Run method.
type SingleTableDelayedRequeuer struct {
	requeuer   *requeuer.Requeuer
	middleware []message.HandlerMiddleware
}

// Middleware returns the middleware that should be added to the router.
func (q SingleTableDelayedRequeuer) Middleware() []message.HandlerMiddleware {
	return q.middleware
}

// Run starts the requeuer.
func (q SingleTableDelayedRequeuer) Run(ctx context.Context) error {
	return q.requeuer.Run(ctx)
}

// SingleTableDelayedRequeuerConfig is a configuration for DelayedRequeuer.
type SingleTableDelayedRequeuerConfig struct {
	// DB is a database connection. Required.
	DB Beginner

	// Publisher is a publisher that will be used to publish requeued messages. Required.
	Publisher message.Publisher

	// RequeueTopic is a topic where requeued messages will be published. Defaults to "requeue".
	RequeueTopic string
	// GeneratePublishTopic is a function that generates the topic where the message should be published after requeue.
	// Defaults to getting the original topic from the message metadata (provided by the PoisonQueue middleware).
	GeneratePublishTopic func(params requeuer.GeneratePublishTopicParams) (string, error)
	// DelayOnError middleware. Optional
	DelayOnError *middleware.DelayOnError

	Logger watermill.LoggerAdapter
}

func (c *SingleTableDelayedRequeuerConfig) setDefaults() {
	if c.RequeueTopic == "" {
		c.RequeueTopic = "requeue"
	}

	if c.GeneratePublishTopic == nil {
		c.GeneratePublishTopic = func(params requeuer.GeneratePublishTopicParams) (string, error) {
			topic := params.Message.Metadata.Get(middleware.PoisonedTopicKey)
			if topic == "" {
				return "", fmt.Errorf("missing topic in metadata")
			}
			return topic, nil
		}
	}

	if c.DelayOnError == nil {
		c.DelayOnError = &middleware.DelayOnError{
			InitialInterval: time.Second * 10,
			MaxInterval:     time.Second * 10,
			Multiplier:      1,
		}
	}

	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c *SingleTableDelayedRequeuerConfig) Validate() error {
	if c.DB == nil {
		return fmt.Errorf("missing db")
	}

	if c.Publisher == nil {
		return fmt.Errorf("missing publisher")
	}

	return nil
}

// NewPostgreSQLSingleTableDelayedRequeuer creates a new DelayedRequeuer that uses PostgreSQL as a storage.
func NewPostgreSQLSingleTableDelayedRequeuer(config SingleTableDelayedRequeuerConfig) (*SingleTableDelayedRequeuer, error) {
	config.setDefaults()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	publisher, err := NewDelayedPostgreSQLPublisher(config.DB, SingleTableDelayedPostgreSQLPublisherConfig{
		Logger: config.Logger,
	})
	if err != nil {
		return nil, err
	}

	subscriber, err := NewDelayedPostgreSQLSubscriber(config.DB, DelayedPostgreSQLSubscriberConfig{
		DeleteOnAck: true,
		Logger:      config.Logger,
	})
	if err != nil {
		return nil, err
	}

	poisonQueue, err := middleware.PoisonQueue(publisher, config.RequeueTopic)
	if err != nil {
		return nil, err
	}

	requeuer, err := requeuer.NewRequeuer(requeuer.Config{
		Subscriber:           subscriber,
		SubscribeTopic:       config.RequeueTopic,
		Publisher:            config.Publisher,
		GeneratePublishTopic: config.GeneratePublishTopic,
	}, config.Logger)
	if err != nil {
		return nil, err
	}

	return &SingleTableDelayedRequeuer{
		middleware: []message.HandlerMiddleware{
			poisonQueue,
			config.DelayOnError.Middleware,
		},
		requeuer: requeuer,
	}, nil
}
