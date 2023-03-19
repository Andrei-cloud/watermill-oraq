package aq

type AQMessage []byte

type Message struct {
	MsgID    string
	Data     []byte
	Metadata map[string]string
}
