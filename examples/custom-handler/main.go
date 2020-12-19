package main

/*
The custom handler, shows how actions can be ovewritten with custom logic.
For example, you can send an example, every time a new user is registered or make an api request.
*/

import (
	"os"

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
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
			"admin": {
				Description: "Admin service",
				Name:        "admin",
				Queue:       "cube-admin",
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
			"card.added": {
				Description: "Propagates a new added payment card (masked pan, expiration data)",
				Stages: []string{
					"backend",
					"billing",
					"admin",
				},
			},
		},
	}, transport, database)
	orchestrator.Run(cubequeue.RoutingTable{
		"account.create": cubequeue.GetDefaultRoutingHandler(),
		"card.added": func(message amqp.Delivery) error {
			logrus.Info("A new card was added")
			return nil
		},
	}, cubequeue.GetDefaultSubscribeSettings(queue))
}
