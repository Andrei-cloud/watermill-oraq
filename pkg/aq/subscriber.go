// Subscriber is a watermill subscriber that reads messages from Oracle Advanced Queue.
package aq

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"text/template"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/godror/godror"
)

// Force Subscriber to implement watermill.Subscriber interface.
var _ message.Subscriber = (*Subscriber)(nil)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

var recreateQueueStmtTmlp string = `DECLARE
tbl CONSTANT VARCHAR2(61) := '{{.Schema}}.QTBL_{{.QueueName}}';
q CONSTANT VARCHAR2(61) := '{{.Schema}}.Q_{{.QueueName}}';
BEGIN
BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL; END;
SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => tbl, sort_list => 'PRIORITY,ENQ_TIME', queue_payload_type => 'RAW',  multiple_consumers => True);
SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '{{.Schema}}');
SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '{{.Schema}}'); 
SYS.DBMS_AQADM.start_queue(q);
dbms_aqadm.add_subscriber( queue_name => q, subscriber =>  sys.aq$_agent('{{.Subscriber}}', null, null),queue_to_queue => false);
END;`

type SubscriberConfig struct {

	// Oracle Advanced queue configuration

	QueueConsumer  string        // Name of the queue subscriber/consumer
	Payload        string        // Payload type
	Transformation string        // Payload oracle transformation name
	QueueWaitTime  time.Duration // Wait time for the queue to return data
	BatchSize      int           // Number of messages to fetch in a single call

	Timeout time.Duration // Timeout for the sql query operation

	Unmarshaler Unmarshaler
}

func (c SubscriberConfig) setDefaults() {

	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.QueueWaitTime == 0 {
		c.QueueWaitTime = 1 * time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if c.QueueConsumer == "" {
		return errors.New("queue consumer is empty")
	}
	if c.QueueWaitTime <= 0 {
		return errors.New("queue wait time must be greater than 0 seconds")
	}
	if c.BatchSize < 1 {
		return errors.New("batch size must be greater than 0")
	}

	return nil
}

type Subscriber struct {
	config SubscriberConfig

	db Transactor

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewSubscriber(db Transactor, config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	config.setDefaults()

	if err := config.validate(); err != nil {
		return nil, errors.Join(err, errors.New("invalid config"))
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		config: config,
		db:     db,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	topic = normalizeTopic(topic)

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	s.db.Close()

	return nil
}

func (s *Subscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.QueueConsumer,
	})

	for {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			return
		default:
		}

		err := s.dequeue(ctx, topic, out, logger)
		if err != nil {
			if oraErr, ok := godror.AsOraErr(err); ok {
				if oraErr.Code() == 24010 {
					logger.Error("while dequeue", oraErr, nil)
					s.Close()
				}
				logger.Error("oracle error", oraErr, nil)
			} else {
				logger.Error("failed to dequeue", err, nil)
			}
		}
	}
}

func (s *Subscriber) dequeue(
	ctx context.Context,
	topic string,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) error {
	var wmessage *message.Message

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(err, errors.New("could not begin tx for dequeueing message"))
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.Error("could not rollback tx for dequeueing message", rollbackErr, nil)
		}
	}()

	q, err := godror.NewQueue(ctx, tx, "Q_"+topic, s.config.Payload,
		godror.WithDeqOptions(godror.DeqOptions{
			Consumer:   s.config.QueueConsumer,
			Mode:       godror.DeqRemove,
			Visibility: godror.VisibleOnCommit,
			Navigation: godror.NavNext,
			Wait:       s.config.QueueWaitTime,
		}))
	if err != nil {
		logger.Error("could not create queue", err, nil)
		return err
	}
	defer q.Close()

	msgs := make([]godror.Message, 1)

	logger.Trace("Querying message", nil)

	n, err := q.Dequeue(msgs)
	if err != nil {
		return err
	}
	logger.Trace("Received message(s)", watermill.LogFields{
		"count": n,
	})

	if n > 0 {
		if s.config.Unmarshaler != nil {
			logger.Trace("Unmarshaling message", watermill.LogFields{
				"payload": string(msgs[0].Raw),
			})
			wmessage, err = s.config.Unmarshaler.Unmarshal(msgs[0].Raw)
			if err != nil {
				return errors.Join(err, errors.New("could not marshal message"))
			}
		} else {
			wmessage = message.NewMessage(watermill.NewUUID(), msgs[0].Raw)
			wmessage.Metadata.Set("OriginID", string(msgs[0].MsgID[:]))
			logger.Trace("processing as raw message", watermill.LogFields{
				"payload":  string(msgs[0].Raw),
				"msg_uuid": wmessage.UUID,
			})
		}

		logger = logger.With(watermill.LogFields{
			"msg_uuid": wmessage.UUID,
		})
		logger.Trace("Received message", nil)

		acked := s.sendMessage(ctx, wmessage, out, logger)
		if !acked {
			logger.Debug("message nacked, rolling back", watermill.LogFields{
				"msg_uuid": wmessage.UUID,
			})
			err := tx.Rollback()
			if err != nil && err != sql.ErrTxDone {
				logger.Error("could not rollback tx for dequeueing message", err, nil)
			}
			return nil
		}
	}
	err = tx.Commit()
	if err != nil && err != sql.ErrTxDone {
		logger.Error("could not commit tx for dequeueing message", err, nil)
		return err
	}

	return nil
}

// sendMessages sends messages on the output channel.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {
	msgCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	msg.SetContext(msgCtx)
	defer cancel()

	for {

		select {
		case out <- msg:

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			logger.Debug("Message nacked, skipping", nil)
			return false

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	if s.closed {
		return ErrSubscriberClosed
	}

	if topic == "" {
		return errors.New("topic is empty")
	}

	ctx := context.Background()

	var schema string
	if err := s.db.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&schema); err != nil {
		s.logger.Error("unable to query schema name", err, nil)
		return err
	}

	stmt, err := s.initQuery(schema, topic)
	if err != nil {
		s.logger.Error("unable to create init query", err, nil)
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		s.logger.Error("unable to begin tx", err, nil)
		return err
	}
	defer func() {
		err := tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			s.logger.Error("unable to rollback tx", err, nil)
		}
	}()
	if _, err := tx.ExecContext(ctx, stmt); err != nil {
		s.logger.Error("unable to init queue", err, nil)
		return err
	}

	s.logger.Info("queue initialized", watermill.LogFields{
		"queue": "Q_" + topic,
	})

	return nil
}

func (s *Subscriber) initQuery(schema, topic string) (string, error) {
	tmpl, err := template.New("initQueue").Parse(recreateQueueStmtTmlp)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(
		&buf,
		map[string]string{"Schema": schema, "QueueName": normalizeTopic(topic), "Subscriber": s.config.QueueConsumer},
	)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
