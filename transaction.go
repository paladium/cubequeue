package cubequeue

import "github.com/paladium/cubequeue/models"

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

// Run functions goes over each routing table item and wraps the function to persist the transaction and notify other services further
func (transactionOrchestrator *TransactionOrchestrator) Run(routingTable RoutingTable, settings SubscribeSettings) {

}
