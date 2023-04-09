package aq

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/godror/godror"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	// Oracle Advanced queue configuration

	QueueConsumer  string // Name of the queue subscriber/consumer
	Payload        string // Payload type
	Transformation string // Payload oracle transformation name

	Marshaler Marshaler
}

func (c PublisherConfig) validate() error {
	if c.QueueConsumer == "" {
		return errors.New("queue consumer is empty")
	}
	return nil
}

// Publisher inserts the Messages as rows into a SQL table..
type Publisher struct {
	config PublisherConfig

	db Transactor

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	logger watermill.LoggerAdapter
}

func NewPublisher(db Transactor, config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Join(err, errors.New("invalid config"))
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		db:     db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,

		logger: logger,
	}, nil
}

// Close closes the publisher, which means that all the Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	topic = normalizeTopic(topic)

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	logger := p.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": p.config.QueueConsumer,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := p.enqueue(ctx, "Q_"+topic, logger, messages...); err != nil {
		if err != nil {
			if oraErr, ok := godror.AsOraErr(err); ok {
				if oraErr.Code() == 24010 {
					logger.Error("while enqueue", oraErr, nil)
					p.Close()
				}
				logger.Error("oracle error", oraErr, nil)
				return oraErr
			} else {
				logger.Error("failed to enqueue", err, nil)
				return err
			}
		}
	}

	return nil
}

func (p *Publisher) enqueue(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	messages ...*message.Message,
) error {
	var bytes []byte
	if p.closed {
		return ErrPublisherClosed
	}

	ora_message := make([]godror.Message, len(messages))

	for i, msg := range messages {
		var err error
		if p.config.Marshaler != nil {
			bytes, err = p.config.Marshaler.Marshal(msg)
			if err != nil {
				return errors.Join(err, errors.New("could not marshal message"))
			}
		} else {
			bytes = msg.Payload
		}

		ora_message[i] = godror.Message{
			Correlation: msg.UUID,
			Raw:         bytes,
			Expiration:  24 * time.Hour,
		}

		if len(ora_message) > 0 {
			tx, err := p.db.BeginTx(ctx, nil)
			if err != nil {
				return errors.Join(err, errors.New("could not begin tx for enqueueing message"))
			}
			defer func() {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil && rollbackErr != sql.ErrTxDone {
					logger.Error("could not rollback tx for dequeueing message", rollbackErr, nil)
				}
			}()

			q, err := godror.NewQueue(ctx, tx, topic, p.config.Payload,
				godror.WithEnqOptions(godror.EnqOptions{
					Transformation: p.config.Transformation,
					Visibility:     godror.VisibleImmediate,
					DeliveryMode:   godror.DeliverPersistent,
				}))
			if err != nil {
				logger.Error("could not create queue", err, nil)
				return err
			}
			defer q.Close()
			if err := q.Enqueue(ora_message); err != nil {
				return errors.Join(err, errors.New("could not enqueue message"))
			}
			err = tx.Commit()
			if err != nil && err != sql.ErrTxDone {
				logger.Error("could not commit tx for dequeueing message", err, nil)
			}
		}
	}

	return nil
}
