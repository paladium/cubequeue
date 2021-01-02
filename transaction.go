package cubequeue

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/paladium/cubequeue/models"
	"github.com/streadway/amqp"
)

// TransactionOrchestrator manager that is responsible for deciding where the message should go next
type TransactionOrchestrator struct {
	transactionConfig *models.TransactionConfig
	transport         *TransactionTransport
	database          ITransactionDatabase
}

// NewTransactionOrchestrator inits the manager
func NewTransactionOrchestrator(
	transactionConfig *models.TransactionConfig,
	transport *TransactionTransport,
	database ITransactionDatabase,
) *TransactionOrchestrator {
	return &TransactionOrchestrator{
		transactionConfig: transactionConfig,
		transport:         transport,
		database:          database,
	}
}

func (transactionOrchestrator *TransactionOrchestrator) ackCurrentStage(transaction *models.TransactionModel, origin string) (*models.TransactionModel, error) {
	if transaction.State().Service != origin {
		return nil, errors.Errorf("The service origin does not match the latest stage - %s != %s", origin, transaction.State().Service)
	}
	if transaction.State().Ack {
		return nil, errors.New("Latest stage is already ack")
	}
	transaction.AckLatestStage()
	transaction, err := transactionOrchestrator.database.Update(transaction.ID, transaction)
	if err != nil {
		return nil, err
	}
	return transaction, nil
}

func (transactionOrchestrator *TransactionOrchestrator) genericTransaction(message amqp.Delivery) (*models.TransactionModel, error) {
	//Check whether the origin header is present
	if _, ok := message.Headers["origin"]; !ok {
		return nil, errors.New("Origin not given")
	}
	origin := message.Headers["origin"].(string)
	//Determine the current service
	service, err := transactionOrchestrator.transactionConfig.FindServiceByName(origin)
	if err != nil {
		return nil, err
	}
	eventType := message.Type
	// Find the transaction first, if it does not exist, record it in db
	transaction, err := transactionOrchestrator.database.Find(message.CorrelationId)
	if err != nil {
		//For now only work with json type
		var body map[string]interface{}
		err = json.Unmarshal(message.Body, &body)
		if err != nil {
			return nil, errors.Wrap(err, "Cannot unmarshal the body")
		}
		//The transaction does not exist, therefore we record it in our database with the origin service being the first one
		transaction, err = transactionOrchestrator.database.Create(&models.TransactionModel{
			ID:      message.CorrelationId,
			Type:    eventType,
			Payload: body,
			Stages: []models.TransactionStageModel{
				{
					Order:   0,
					Queue:   service.Queue,
					Service: origin,
					Date:    time.Now(),
					Ack:     true,
				},
			},
		})
		if err != nil {
			return nil, err
		}
	} else {
		//Otherwise simply ack the current service for the transaction
		if transaction.State().Service != origin {
			return nil, errors.Wrapf(errors.New("The service origin does not match the latest stage"), "%s != %s", origin, transaction.State().Service)
		}
		transaction, err = transactionOrchestrator.ackCurrentStage(transaction, origin)
		if err != nil {
			return nil, err
		}
	}
	return transaction, nil
}

// Resolve the transaction and determine what should happen next based on the transaction configuration
func (transactionOrchestrator *TransactionOrchestrator) handleTransaction(message amqp.Delivery) error {
	transaction, err := transactionOrchestrator.genericTransaction(message)
	if err != nil {
		return err
	}
	if _, ok := transactionOrchestrator.transactionConfig.Transactions[transaction.Type]; !ok {
		return errors.New("Transaction type cannot be found")
	}
	currentTransactionConfig := transactionOrchestrator.transactionConfig.Transactions[transaction.Type]
	if !transaction.State().Ack {
		return errors.New("The previous service did not send the ack")
	}
	transactionChain, err := models.NewTransactionChain(*transactionOrchestrator.transactionConfig, currentTransactionConfig)
	if err != nil {
		return err
	}
	nextService, err := transactionChain.NextService(transaction)
	if err != nil {
		return err
	}
	//Publish the message to this next service
	body, err := json.Marshal(transaction.Payload)
	if err != nil {
		return errors.Wrap(err, "Cannot marshal the body")
	}
	//Save the stage to db
	transaction.AddStage(models.TransactionStageModel{
		Queue:   nextService.Queue,
		Service: nextService.Name,
		Ack:     false,
		Date:    time.Now(),
	})
	transaction, err = transactionOrchestrator.database.Update(transaction.ID, transaction)
	if err != nil {
		return err
	}
	err = transactionOrchestrator.transport.Publish(nextService.Queue, amqp.Publishing{
		Type:          transaction.Type,
		CorrelationId: transaction.ID,
		Body:          body,
	})
	if err != nil {
		return err
	}
	return nil
}

func (transactionOrchestrator *TransactionOrchestrator) rollback(transaction *models.TransactionModel) error {
	//Notify all the previous services about the rollback
	latestStageIndex := len(transaction.Stages) - 1
	for i := 0; i < latestStageIndex; i++ {
		stage := transaction.Stages[i]
		err := transactionOrchestrator.transport.Publish(stage.Queue, amqp.Publishing{
			Type: RollbackMessage,
			Headers: amqp.Table{
				"error": *transaction.State().Error,
			},
			CorrelationId: transaction.ID,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (transactionOrchestrator *TransactionOrchestrator) handleError(message amqp.Delivery) error {
	transaction, err := transactionOrchestrator.genericTransaction(message)
	if err != nil {
		return err
	}
	if transaction.State().Error != nil {
		return errors.New("Latest stage already has an error")
	}
	if _, ok := message.Headers["error"]; !ok {
		return errors.New("Error message not given")
	}
	//Set the error on the latest stage and update the transaction in database
	errorMessage := message.Headers["error"].(string)
	transaction.SetErrorLatestStage(errorMessage)
	transaction, err = transactionOrchestrator.database.Update(transaction.ID, transaction)
	if err != nil {
		return err
	}
	//Finally send the rollback message to all the previous services
	err = transactionOrchestrator.rollback(transaction)
	if err != nil {
		return err
	}
	return nil
}

// Run functions goes over each routing table item and wraps the function to persist the transaction and notify other services further
func (transactionOrchestrator *TransactionOrchestrator) Run(routingTable RoutingTable, settings SubscribeSettings) error {
	for key, handler := range routingTable {
		routingTable[key] = func(message amqp.Delivery) error {
			//Save transaction or update current status of it
			err := transactionOrchestrator.handleTransaction(message)
			if err != nil {
				return err
			}
			err = handler(message)
			if err != nil {
				return err
			}
			return nil
		}
	}
	//Add the error handling route
	routingTable[ErrorMessage] = transactionOrchestrator.handleError
	logrus.Debug("Running the orchestrator")
	err := transactionOrchestrator.transport.Subscribe(routingTable, settings)
	if err != nil {
		return err
	}
	return nil
}

// Close all connections
func (transactionOrchestrator *TransactionOrchestrator) Close() {
	transactionOrchestrator.transport.Close()
	transactionOrchestrator.database.Close()
}
