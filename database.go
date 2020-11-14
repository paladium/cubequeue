package cubequeue

import "github.com/paladium/cubequeue/models"

// ITransactionDatabase is a contract that has to be implemented in order to allow for persistence of the transactions
type ITransactionDatabase interface {
	Find(id string) (*models.TransactionModel, error)
	Create(transaction *models.TransactionModel) (*models.TransactionModel, error)
	Update(id string, transaction *models.TransactionModel) (*models.TransactionModel, error)
	Close()
}
