package main

/*
The custom handler, shows how actions can be ovewritten with custom logic.
For example when you receive account.create message you should create a new account and when you receive rollback message, you should delete the created account
*/

import (
	"os"

	"github.com/mitchellh/mapstructure"
	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/client"
	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
	"github.com/pkg/errors"
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
		"account.create": func(transaction *models.TransactionModel) error {
			//Save the account to database
			createAccount := struct {
				AccountName string `json:"accountName"`
				AccountID   string `json:"accountId"`
			}{}
			err = mapstructure.Decode(transaction.Payload, &createAccount)
			if err != nil {
				return errors.Wrap(err, "Cannot decode the map")
			}
			logrus.Infof("Adding new account id=%s name=%s", createAccount.AccountID, createAccount.AccountName)
			return nil
		},
	}, client.TransactionRoutingTable{
		"account.create": func(transaction *models.TransactionModel) error {
			//Save the account to database
			createAccount := struct {
				AccountName string `json:"accountName"`
				AccountID   string `json:"accountId"`
			}{}
			err = mapstructure.Decode(transaction.Payload, &createAccount)
			if err != nil {
				return errors.Wrap(err, "Cannot decode the map")
			}
			logrus.Infof("Deleting account due to error id=%s name=%s", createAccount.AccountID, createAccount.AccountName)
			return nil
		},
	})
}
