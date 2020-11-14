package cubequeue

import (
	"testing"

	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/stretchr/testify/assert"
)

func TestCanRunTransactionOrchestrator(t *testing.T) {
	transport, err := NewTransactionTransport(TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: GetDefaultQueueSetting("cubequeue"),
	})
	assert.Nil(t, err)
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://guest:guest@localhost:27017", "cubequeue", "transactions")
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
	go orchestrator.Run(RoutingTable{
		"invoice.create": GetDefaultRoutingHandler(),
	}, GetDefaultSubscribeSettings("cubequeue"))
	orchestrator.Close()
}
