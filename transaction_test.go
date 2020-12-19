package cubequeue

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestCanRunTransactionOrchestrator(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))
}

func TestExecuteNewTransaction(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))

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

	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "backend",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("fa621107-5b79-4e8b-9587-df064f1052b4")
	assert.Nil(t, err)
	assert.Equal(t, "fa621107-5b79-4e8b-9587-df064f1052b4", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
	assert.Len(t, transaction.Stages, 2)
	assert.Equal(t, "cube-backend", transaction.Stages[0].Queue)
	assert.Equal(t, "backend", transaction.Stages[0].Service)
	assert.Equal(t, 0, transaction.Stages[0].Order)
	assert.Nil(t, transaction.Stages[0].Error)
	assert.Equal(t, true, transaction.Stages[0].Ack)

	//Now check for second stage of the transaction
	assert.Equal(t, "cube-billing", transaction.Stages[1].Queue)
	assert.Equal(t, "billing", transaction.Stages[1].Service)
	assert.Equal(t, 1, transaction.Stages[1].Order)
	assert.Nil(t, transaction.Stages[1].Error)
	assert.Equal(t, false, transaction.Stages[1].Ack)
}

func TestCanAckExistingTransaction(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))

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

	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "backend",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we publish the message from billing service, that we have finished processing the transaction
	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "billing",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("fa621107-5b79-4e8b-9587-df064f1052b4")
	assert.Nil(t, err)
	assert.Equal(t, "fa621107-5b79-4e8b-9587-df064f1052b4", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
	assert.Len(t, transaction.Stages, 3)
	assert.Equal(t, "cube-backend", transaction.Stages[0].Queue)
	assert.Equal(t, "backend", transaction.Stages[0].Service)
	assert.Equal(t, 0, transaction.Stages[0].Order)
	assert.Nil(t, transaction.Stages[0].Error)
	assert.Equal(t, true, transaction.Stages[0].Ack)

	//Now check for second stage of the transaction
	assert.Equal(t, "cube-billing", transaction.Stages[1].Queue)
	assert.Equal(t, "billing", transaction.Stages[1].Service)
	assert.Equal(t, 1, transaction.Stages[1].Order)
	assert.Nil(t, transaction.Stages[1].Error)
	assert.Equal(t, true, transaction.Stages[1].Ack)

	//Now check for third stage of the transaction
	assert.Equal(t, "cube-admin", transaction.Stages[2].Queue)
	assert.Equal(t, "admin", transaction.Stages[2].Service)
	assert.Equal(t, 2, transaction.Stages[2].Order)
	assert.Nil(t, transaction.Stages[2].Error)
	assert.Equal(t, false, transaction.Stages[2].Ack)
}

func TestCanFinishExistingTransaction(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))

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

	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "backend",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we publish the message from billing service, that we have finished processing the transaction
	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "billing",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Finally we publish success message from admin service
	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "admin",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("fa621107-5b79-4e8b-9587-df064f1052b4")
	assert.Nil(t, err)
	assert.Equal(t, "fa621107-5b79-4e8b-9587-df064f1052b4", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
	assert.Len(t, transaction.Stages, 3)
	assert.Equal(t, "cube-backend", transaction.Stages[0].Queue)
	assert.Equal(t, "backend", transaction.Stages[0].Service)
	assert.Equal(t, 0, transaction.Stages[0].Order)
	assert.Nil(t, transaction.Stages[0].Error)
	assert.Equal(t, true, transaction.Stages[0].Ack)

	//Now check for second stage of the transaction
	assert.Equal(t, "cube-billing", transaction.Stages[1].Queue)
	assert.Equal(t, "billing", transaction.Stages[1].Service)
	assert.Equal(t, 1, transaction.Stages[1].Order)
	assert.Nil(t, transaction.Stages[1].Error)
	assert.Equal(t, true, transaction.Stages[1].Ack)

	//Now check for third stage of the transaction
	assert.Equal(t, "cube-admin", transaction.Stages[2].Queue)
	assert.Equal(t, "admin", transaction.Stages[2].Service)
	assert.Equal(t, 2, transaction.Stages[2].Order)
	assert.Nil(t, transaction.Stages[2].Error)
	assert.Equal(t, true, transaction.Stages[2].Ack)
}

func TestCanErrorExistingTransaction(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))

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

	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "backend",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we publish the message from billing service, that we have finished processing the transaction
	errorString := "The invoice with the same number already exists"
	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "error",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "billing",
			"error":  errorString,
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("fa621107-5b79-4e8b-9587-df064f1052b4")
	assert.Nil(t, err)
	assert.Equal(t, "fa621107-5b79-4e8b-9587-df064f1052b4", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
	assert.Len(t, transaction.Stages, 2)
	assert.Equal(t, "cube-backend", transaction.Stages[0].Queue)
	assert.Equal(t, "backend", transaction.Stages[0].Service)
	assert.Equal(t, 0, transaction.Stages[0].Order)
	assert.Nil(t, transaction.Stages[0].Error)
	assert.Equal(t, true, transaction.Stages[0].Ack)

	//Now check for second stage of the transaction
	assert.Equal(t, "cube-billing", transaction.Stages[1].Queue)
	assert.Equal(t, "billing", transaction.Stages[1].Service)
	assert.Equal(t, 1, transaction.Stages[1].Order)
	assert.Equal(t, errorString, *transaction.Stages[1].Error)
	assert.Equal(t, true, transaction.Stages[1].Ack)
}

func TestCannotAckNextService(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	assert.Nil(t, err)
	orchestrator := NewTransactionOrchestrator(&models.TransactionConfig{
		Services: map[string]models.TransactionService{
			"backend": {
				Description: "Main backend",
				Name:        "backend",
				Queue:       "cube-backend",
			},
			"billing": {
				Description: "Billing service",
				Name:        "billing",
				Queue:       "cube-billing",
			},
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
			},
			"image-processor": {
				Description: "Image processing service",
				Name:        "image-processor",
				Queue:       "cube-image-processor",
			},
		},
		Transactions: map[string]models.Transaction{
			"invoice.create": {
				Description: "Transaction for invoicing a customer",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	assert.NotNil(t, orchestrator)
	defer orchestrator.Close()
	defer database.DeleteDatabase()
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))

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

	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "backend",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we publish the message from billing service, that we have finished processing the transaction
	transport.Publish("cubequeue", amqp.Publishing{
		CorrelationId: "fa621107-5b79-4e8b-9587-df064f1052b4",
		Type:          "invoice.create",
		Body:          messageBody,
		Headers: amqp.Table{
			"origin": "admin",
		},
	})
	//Simulate wait to process the message
	time.Sleep(5 * time.Second)
	//Next we find our transaction in db and verify its fields
	transaction, err := database.Find("fa621107-5b79-4e8b-9587-df064f1052b4")
	assert.Nil(t, err)
	assert.Equal(t, "fa621107-5b79-4e8b-9587-df064f1052b4", transaction.ID)
	assert.Equal(t, "invoice.create", transaction.Type)
	assert.Equal(t, map[string]interface{}{
		"invoiceNumber": "34555678",
		"filename":      "invoice-34555678.pdf",
		"amount":        56.67,
	}, transaction.Payload)
	assert.Len(t, transaction.Stages, 2)
	assert.Equal(t, "cube-backend", transaction.Stages[0].Queue)
	assert.Equal(t, "backend", transaction.Stages[0].Service)
	assert.Equal(t, 0, transaction.Stages[0].Order)
	assert.Nil(t, transaction.Stages[0].Error)
	assert.Equal(t, true, transaction.Stages[0].Ack)

	//Now check for second stage of the transaction
	assert.Equal(t, "cube-billing", transaction.Stages[1].Queue)
	assert.Equal(t, "billing", transaction.Stages[1].Service)
	assert.Equal(t, 1, transaction.Stages[1].Order)
	assert.Nil(t, transaction.Stages[1].Error)
	assert.Equal(t, false, transaction.Stages[1].Ack)
}
