package aq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/andrei-cloud/watermill-oraq/pkg/aq"
	"github.com/godror/godror"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, false)
)

// todo: move hardcoded credentials to env variables
const (
	ORA_USER = "TCTDBS"
	ORA_PASS = "TCTDBS"
	LIB_PATH = "/Users/andrei/oracle/instantclient_12_2"
	ORA_TNS  = "localhost:1521/xe"
)

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ExactlyOnceDelivery:                 true,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "ACQ")
}

func newOracle(tns, user, pass string) (*sql.DB, error) {
	params := &godror.ConnectionParams{}
	params.Username, params.Password = user, godror.NewPassword(pass)
	params.ConnectString = tns
	params.LibDir = LIB_PATH
	params.StandaloneConnection = false
	params.NoTZCheck = true

	logger.Info("attempting to connect", watermill.LogFields{
		"tns": params.ConnectString,
	})

	db := sql.OpenDB(godror.NewConnector(*params))

	// Set a maximum connection lifetime of 1 hour
	db.SetConnMaxLifetime(time.Hour)

	// Set a maximum connection idle of 10
	db.SetMaxIdleConns(10)

	// Set a maximum open connection limit of 100
	db.SetMaxOpenConns(100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		return nil, err
	}
	defer cancel()
	return db, nil
}

func newPubSub(
	t *testing.T,
	consumerGroup string,
) (message.Publisher, message.Subscriber) {
	pub_db, err := newOracle(ORA_TNS, ORA_USER, ORA_PASS)
	require.NoError(t, err)

	publisher, err := aq.NewPublisher(
		pub_db,
		aq.PublisherConfig{
			QueueConsumer:  consumerGroup,
			Payload:        "",
			Transformation: "",
			Marshaler:      aq.JSONMarshaler{},
		},
		logger,
	)
	require.NoError(t, err)

	sub_db, err := newOracle(ORA_TNS, ORA_USER, ORA_PASS)
	require.NoError(t, err)
	subscriber, err := aq.NewSubscriber(
		sub_db,
		aq.SubscriberConfig{
			QueueConsumer:  consumerGroup,
			Payload:        "",
			Transformation: "",
			BatchSize:      1,
			Unmarshaler:    aq.JSONMarshaler{},
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup)
}
