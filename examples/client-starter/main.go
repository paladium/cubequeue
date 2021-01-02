package main

/*
The starter example - minimum example that shows how to use client part of the library to execute and process transactions
*/
import (
	"os"

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/client"
	"github.com/paladium/cubequeue/databases"
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
	queue := "billing"
	//Setup the logging
	setLogging()
	transport, err := cubequeue.NewTransactionTransport(cubequeue.TransactionTransportConnectionSetting{
		URL:   "amqp://guest:guest@localhost:5672",
		Queue: cubequeue.GetDefaultQueueSetting(queue),
	})
	if err != nil {
		panic(err)
	}
	database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "billing", "transactions")
	if err != nil {
		panic(err)
	}
	//Configure the ServiceName - the same name should be used for your orchestrator
	//TransactionQueue - where to publish the transaction to after processing it
	//SubscribeSettings - how to subscribe to the queue
	worker := client.NewBackgroundWorker(transport, database, &client.BackgroundWorkerSettings{
		ServiceName:       "billing",
		TransactionQueue:  "cubequeue",
		SubscribeSettings: cubequeue.GetDefaultSubscribeSettings(queue),
	})
	worker.Run(client.TransactionRoutingTable{
		"account.create": client.GetDefaultTransactionRoutingHandler(),
	}, client.TransactionRoutingTable{
		"account.create": client.GetDefaultTransactionRoutingHandler(),
	})
}
