package aq

import (
	"encoding/json"
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Marshaler provides transport encoding functions
type Marshaler interface {
	// Marshal transforms a watermill message into NATS wire format.
	Marshal(msg *message.Message) (AQMessage, error)
}

// Unmarshaler provides transport decoding function
type Unmarshaler interface {
	// Unmarshal produces a watermill message from NATS wire format.
	Unmarshal(AQMessage) (*message.Message, error)
}

// MarshalerUnmarshaler provides both Marshaler and Unmarshaler implementations
type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// JSONMarshaler uses encoding/json to marshal Watermill messages.
type JSONMarshaler struct{}

// Marshal transforms a watermill message into JSON format.
func (JSONMarshaler) Marshal(msg *message.Message) (AQMessage, error) {
	aqMsg := Message{
		MsgID:    msg.UUID,
		Data:     msg.Payload,
		Metadata: msg.Metadata,
	}
	bytes, err := json.Marshal(aqMsg)
	if err != nil {
		return nil, errors.Join(err, errors.New("cannot encode message"))
	}

	return bytes, nil
}

// Unmarshal extracts a watermill message from a nats message.
func (JSONMarshaler) Unmarshal(aqMsg AQMessage) (*message.Message, error) {
	var decodedMsg Message
	err := json.Unmarshal(aqMsg, &decodedMsg)
	if err != nil {
		return nil, errors.Join(err, errors.New("cannot decode message"))
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.MsgID, decodedMsg.Data)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}
