package main

/*
The starter example, that you can plug into your orchestrator and start writing handlers
The starter example uses mongodb as persistence and rabbitmq as transport
You can configure as many transaction types
The custom-handler example demonstrates how to override the default handler with custom logic
*/
import (
	"os"

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/sirupsen/logrus"
)

func setLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)
}

func main() {
	queue := "cubequeue"
	//Setup the logging
	setLogging()
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting(queue),
	})
	if err != nil {
		panic(err)
	}
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
	if err != nil {
		panic(err)
	}
	orchestrator := cubequeue.NewTransactionOrchestrator(&models.TransactionConfig{
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
		},
		Transactions: map[string]models.Transaction{
			"account.create": {
				Description: "Create a new account",
				Stages: []string{
					"backend",
					"billing",
				},
			},
		},
	}, transport, database)
	orchestrator.Run(cubequeue.RoutingTable{
		"account.create": cubequeue.GetDefaultRoutingHandler(),
	}, cubequeue.GetDefaultSubscribeSettings(queue))
}
