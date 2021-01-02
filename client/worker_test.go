package client

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestCanRunBackgroundWorker(t *testing.T) {
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting("billing"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	assert.Nil(t, err)
	worker := NewBackgroundWorker(transport, database, &BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("billing"),
	})
	assert.NotNil(t, worker)
	defer worker.Close()
	defer database.DeleteDatabase()
	go worker.Run(TransactionRoutingTable{
		"invoice.create": GetDefaultTransactionRoutingHandler(),
	}, TransactionRoutingTable{
		"invoice.create": GetDefaultTransactionRoutingHandler(),
	})
}

func TestCanExecuteExistingTransaction(t *testing.T) {
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting("billing"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	assert.Nil(t, err)
	worker := NewBackgroundWorker(transport, database, &BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("billing"),
	})
	assert.NotNil(t, worker)
	defer worker.Close()
	defer database.DeleteDatabase()
	go worker.Run(TransactionRoutingTable{
		"invoice.create": func(transaction *models.TransactionModel) error {
			assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
			return nil
		},
	}, TransactionRoutingTable{
		"invoice.create": GetDefaultTransactionRoutingHandler(),
	})

	createInvoice := struct {
		InvoiceNumber string  `json:"invoiceNumber"`
		Filename      string  `json:"filename"`
		Amount        float64 `json:"amount"`
	}{
		InvoiceNumber: "34555678",
		Filename:      "invoice-34555678.pdf",
		Amount:        56.67,
	}
	messageBody, err := json.Marshal(createInvoice)
	assert.Nil(t, err)

	transport.Publish("billing", amqp.Publishing{
		CorrelationId: "82941436-9940-42c9-9f30-9f82a0861457",
		Type:          "invoice.create",
		Body:          messageBody,
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("82941436-9940-42c9-9f30-9f82a0861457")
	assert.Nil(t, err)
	assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
}

func TestCanExecuteNewTransaction(t *testing.T) {
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting("billing"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	assert.Nil(t, err)
	worker := NewBackgroundWorker(transport, database, &BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("billing"),
	})
	assert.NotNil(t, worker)
	defer worker.Close()
	defer database.DeleteDatabase()
	go worker.Run(TransactionRoutingTable{
		"account.create": func(transaction *models.TransactionModel) error {
			assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
			return nil
		},
	}, TransactionRoutingTable{
		"account.create": GetDefaultTransactionRoutingHandler(),
	})

	createAccount := struct {
		AccountName string `json:"accountName"`
		AccountID   string `json:"accountId"`
	}{
		AccountName: "Apple INC",
		AccountID:   "2345672",
	}
	messageBody, err := json.Marshal(createAccount)
	assert.Nil(t, err)

	transport.Publish("billing", amqp.Publishing{
		CorrelationId: "82941436-9940-42c9-9f30-9f82a0861457",
		Type:          "account.create",
		Body:          messageBody,
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("82941436-9940-42c9-9f30-9f82a0861457")
	assert.Nil(t, err)
	assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
	assert.Equal(t, "account.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"accountName": "Apple INC",
		"accountId":   "2345672",
	}, transaction.Payload)
}

func TestCanFailNewTransaction(t *testing.T) {
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting("billing"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	assert.Nil(t, err)
	worker := NewBackgroundWorker(transport, database, &BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("billing"),
	})
	assert.NotNil(t, worker)
	defer worker.Close()
	defer database.DeleteDatabase()
	go worker.Run(TransactionRoutingTable{
		"account.create": func(transaction *models.TransactionModel) error {
			return errors.New("Cannot create the account. The account id is not unique")
		},
	}, TransactionRoutingTable{
		"account.create": GetDefaultTransactionRoutingHandler(),
	})

	createAccount := struct {
		AccountName string `json:"accountName"`
		AccountID   string `json:"accountId"`
	}{
		AccountName: "Apple INC",
		AccountID:   "2345672",
	}
	messageBody, err := json.Marshal(createAccount)
	assert.Nil(t, err)

	transport.Publish("billing", amqp.Publishing{
		CorrelationId: "82941436-9940-42c9-9f30-9f82a0861457",
		Type:          "account.create",
		Body:          messageBody,
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("82941436-9940-42c9-9f30-9f82a0861457")
	assert.Nil(t, err)
	assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
	assert.Equal(t, "account.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"accountName": "Apple INC",
		"accountId":   "2345672",
	}, transaction.Payload)
}

func TestCanRollbackNewTransaction(t *testing.T) {
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting("billing"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	assert.Nil(t, err)
	worker := NewBackgroundWorker(transport, database, &BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("billing"),
	})
	assert.NotNil(t, worker)
	defer worker.Close()
	defer database.DeleteDatabase()
	go worker.Run(TransactionRoutingTable{
		"account.create": func(transaction *models.TransactionModel) error {
			assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
			return nil
		},
	}, TransactionRoutingTable{
		"account.create": func(transaction *models.TransactionModel) error {
			assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
			return nil
		},
	})

	createAccount := struct {
		AccountName string `json:"accountName"`
		AccountID   string `json:"accountId"`
	}{
		AccountName: "Apple INC",
		AccountID:   "2345672",
	}
	messageBody, err := json.Marshal(createAccount)
	assert.Nil(t, err)

	transport.Publish("billing", amqp.Publishing{
		CorrelationId: "82941436-9940-42c9-9f30-9f82a0861457",
		Type:          "account.create",
		Body:          messageBody,
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("82941436-9940-42c9-9f30-9f82a0861457")
	assert.Nil(t, err)
	assert.Equal(t, "82941436-9940-42c9-9f30-9f82a0861457", transaction.ID)
	assert.Equal(t, "account.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"accountName": "Apple INC",
		"accountId":   "2345672",
	}, transaction.Payload)
	//Next publish the rollback message
	transport.Publish("billing", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "rollback",
		Headers: amqp.Table{
			"error": "The account already exists",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
}
