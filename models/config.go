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
