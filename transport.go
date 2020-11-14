package cubequeue

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// TransactionTransport is responsible for publishing messages to amqp (for now)
type TransactionTransport struct {
	consumer, publisher *amqp.Channel
	connection          *amqp.Connection
	queue               amqp.Queue
}

// TransactionTransportConnectionQueueSettings stores settings for declaring a listening queue
// The most important one is the queue name, other settings can be left to default
type TransactionTransportConnectionQueueSettings struct {
	QueueName  string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// GetDefaultQueueSetting returns default settings, suitable for most cases
func GetDefaultQueueSetting(queueName string) TransactionTransportConnectionQueueSettings {
	return TransactionTransportConnectionQueueSettings{
		QueueName:  queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

// TransactionTransportConnectionSetting stores connection settings for the transport
type TransactionTransportConnectionSetting struct {
	URL   string
	Queue TransactionTransportConnectionQueueSettings
}

// RoutingTableHandler func for handling the event
type RoutingTableHandler func(amqp.Delivery) error

// RoutingTable is used to route the requests from amqp
type RoutingTable map[string]RoutingTableHandler

// GetDefaultRoutingHandler returns default - empty handler
func GetDefaultRoutingHandler() RoutingTableHandler {
	return func(message amqp.Delivery) error { return nil }
}

// Important handlers the routing table
const (
	NoHandlerMessage = "no_handler"
)

// SubscribeSettings contains settings when subscribing to the queue
type SubscribeSettings struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// GetDefaultSubscribeSettings returns default settings
func GetDefaultSubscribeSettings(queue string) SubscribeSettings {
	return SubscribeSettings{
		Queue:     queue,
		Consumer:  "",
		AutoAck:   true,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}
}

// NewTransactionTransport create a new transport
func NewTransactionTransport(connectionSetting TransactionTransportConnectionSetting) (*TransactionTransport, error) {
	transport := TransactionTransport{}
	var err error
	transport.connection, err = amqp.Dial(connectionSetting.URL)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot connect to the amqp")
	}
	transport.consumer, err = transport.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create a consumer channel")
	}
	transport.publisher, err = transport.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create a publisher channel")
	}
	//Now declare the queue we will listen on for the incoming messages
	transport.queue, err = transport.consumer.QueueDeclare(
		connectionSetting.Queue.QueueName,
		connectionSetting.Queue.Durable,
		connectionSetting.Queue.AutoDelete,
		connectionSetting.Queue.Exclusive,
		connectionSetting.Queue.NoWait,
		connectionSetting.Queue.Args,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot declare the queue for messages")
	}
	return &transport, nil
}

// Publish a message to a given queue
func (transport TransactionTransport) Publish(queue string, message amqp.Publishing) error {
	err := transport.publisher.Publish("", queue, false, false, message)
	if err != nil {
		return errors.Wrap(err, "Cannot publish a message to the queue")
	}
	return nil
}

// Subscribe for messages in current queue
func (transport TransactionTransport) subscribe(routingTable RoutingTable, settings SubscribeSettings) error {
	messages, err := transport.consumer.Consume(
		settings.Queue,
		settings.Consumer,
		settings.AutoAck,
		settings.Exclusive,
		settings.NoLocal,
		settings.NoWait,
		settings.Args,
	)
	if err != nil {
		return errors.Wrap(err, "Cannot register a consumer")
	}
	for message := range messages {
		if _, ok := routingTable[message.Type]; !ok {
			//No handler for the message, then use the no_handler handler
			routingTable[NoHandlerMessage](message)
		} else {
			routingTable[message.Type](message)
		}
	}
	return nil
}

// Close close all connections
func (transport TransactionTransport) close() {
	transport.consumer.Close()
	transport.publisher.Close()
	transport.connection.Close()
}
