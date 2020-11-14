package models

import "github.com/pkg/errors"

// TransactionService stores one of the services that messages could be delivered to
type TransactionService struct {
	Description string
	Queue       string
	Name        string
}

// Transaction is a single transaction that has a number of stages it has to go through
type Transaction struct {
	Description string
	Stages      []string
}

// TransactionConfig stores the current available services & transactions
type TransactionConfig struct {
	Services     map[string]TransactionService
	Transactions map[string]Transaction
}

// FindServiceByName finds the service by its name
func (transactionConfig TransactionConfig) FindServiceByName(name string) (*TransactionService, error) {
	if service, ok := transactionConfig.Services[name]; ok {
		return &service, nil
	}
	return nil, errors.New("Service cannot be found")
}

// TransactionChain contains each service one by one resolved from a particular transaction type (we only have the names of the services)
type TransactionChain []TransactionService

// NextService returns the next service to be handled in a transaction
func (chain TransactionChain) NextService(transaction *TransactionModel) (*TransactionService, error) {
	latestIndex := len(transaction.Stages) - 1
	index := latestIndex + 1
	if index > len(chain)-1 {
		return nil, errors.New("No more steps")
	}
	return &chain[index], nil
}

// NewTransactionChain makes a new transaction chain based on a particular transaction`s config
func NewTransactionChain(transactionConfig TransactionConfig, transaction Transaction) (TransactionChain, error) {
	chain := []TransactionService{}
	for _, stage := range transaction.Stages {
		service, err := transactionConfig.FindServiceByName(stage)
		if err != nil {
			return nil, errors.Errorf("Cannot find a service by its name %s", stage)
		}
		chain = append(chain, *service)
	}
	return chain, nil
}
