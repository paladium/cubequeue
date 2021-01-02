package client

import (
	"encoding/json"

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// BackgroundWorkerSettings stores settings to configure background worker
type BackgroundWorkerSettings struct {
	TransactionQueue  string
	ServiceName       string
	SubscribeSettings cubequeue.SubscribeSettings
}

// BackgroundWorker responsible for receiving background messages and processing transactions
type BackgroundWorker struct {
	transport *cubequeue.TransactionTransport
	database  cubequeue.ITransactionDatabase
	settings  *BackgroundWorkerSettings
}

// TransactionRoutingTableHandler func for handling the transaction
type TransactionRoutingTableHandler func(*models.TransactionModel) error

// TransactionRoutingTable is used to route the requests from amqp
type TransactionRoutingTable map[string]TransactionRoutingTableHandler

// GetDefaultTransactionRoutingHandler returns default - empty handler
func GetDefaultTransactionRoutingHandler() TransactionRoutingTableHandler {
	return func(transaction *models.TransactionModel) error { return nil }
}

// NewBackgroundWorker inits the worker
func NewBackgroundWorker(
	transport *cubequeue.TransactionTransport,
	database cubequeue.ITransactionDatabase,
	settings *BackgroundWorkerSettings,
) *BackgroundWorker {
	return &BackgroundWorker{
		transport: transport,
		database:  database,
		settings:  settings,
	}
}

func (backgroundWorker *BackgroundWorker) genericTransaction(message amqp.Delivery) (*models.TransactionModel, error) {
	// Find the transaction first, if it does not exist, record it in db
	transaction, err := backgroundWorker.database.Find(message.CorrelationId)
	if err != nil {
		//For now only work with json type
		var body map[string]interface{}
		err = json.Unmarshal(message.Body, &body)
		if err != nil {
			return nil, errors.Wrap(err, "Cannot unmarshal the body")
		}
		transaction, err = backgroundWorker.database.Create(&models.TransactionModel{
			ID:      message.CorrelationId,
			Type:    message.Type,
			Payload: body,
		})
		if err != nil {
			return nil, err
		}
	}
	return transaction, nil
}

func (backgroundWorker *BackgroundWorker) publishErrorMessage(transaction *models.TransactionModel, errorMessage string) error {
	return backgroundWorker.transport.Publish(backgroundWorker.settings.TransactionQueue, amqp.Publishing{
		CorrelationId: transaction.ID,
		Type:          transaction.Type,
		Headers: amqp.Table{
			"origin": backgroundWorker.settings.ServiceName,
			"error":  errorMessage,
		},
	})
}

func (backgroundWorker *BackgroundWorker) continueTransaction(transaction *models.TransactionModel) error {
	body, err := json.Marshal(transaction.Payload)
	if err != nil {
		return errors.Wrap(err, "Cannot marshal the json")
	}
	return backgroundWorker.transport.Publish(backgroundWorker.settings.TransactionQueue, amqp.Publishing{
		CorrelationId: transaction.ID,
		Type:          transaction.Type,
		Body:          body,
		Headers: amqp.Table{
			"origin": backgroundWorker.settings.ServiceName,
		},
	})
}

func (backgroundWorker *BackgroundWorker) handleTransaction(message amqp.Delivery, handler TransactionRoutingTableHandler) error {
	transaction, err := backgroundWorker.genericTransaction(message)
	if err != nil {
		return backgroundWorker.publishErrorMessage(transaction, err.Error())
	}
	err = handler(transaction)
	if err != nil {
		return backgroundWorker.publishErrorMessage(transaction, err.Error())
	}
	return backgroundWorker.continueTransaction(transaction)
}

// This function wraps around handler for rollbacks and executes handler for the transaction that matches the one that should be rolled back
func (backgroundWorker *BackgroundWorker) handleRollback(rollbackTable TransactionRoutingTable) func(message amqp.Delivery) error {
	return func(message amqp.Delivery) error {
		// Find the transaction first, if it does not exist, record it in db
		transaction, err := backgroundWorker.database.Find(message.CorrelationId)
		if err != nil {
			return err
		}
		err = rollbackTable[transaction.Type](transaction)
		if err != nil {
			return err
		}
		return nil
	}
}

// Run the background worker
func (backgroundWorker *BackgroundWorker) Run(transactionRoutingTable TransactionRoutingTable, rollbackTable TransactionRoutingTable) error {
	routingTable := cubequeue.RoutingTable{}
	for key, handler := range transactionRoutingTable {
		routingTable[key] = func(message amqp.Delivery) error {
			//Save transaction or update current status of it
			err := backgroundWorker.handleTransaction(message, handler)
			if err != nil {
				return err
			}
			return nil
		}
	}
	//Add the rollback handling route
	routingTable[cubequeue.RollbackMessage] = backgroundWorker.handleRollback(rollbackTable)
	logrus.Debug("Running the worker")
	err := backgroundWorker.transport.Subscribe(routingTable, backgroundWorker.settings.SubscribeSettings)
	if err != nil {
		return err
	}
	return nil
}

// Close all connections
func (backgroundWorker *BackgroundWorker) Close() {
	backgroundWorker.transport.Close()
	backgroundWorker.database.Close()
}
