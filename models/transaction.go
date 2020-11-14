package models

import "time"

// TransactionStageModel is individual stage of transaction
type TransactionStageModel struct {
	Order   int
	Service string
	Queue   string
	Ack     bool
	Date    time.Time
	Error   *string
}

// TransactionModel represents a single transaction that keeps track of its stages
type TransactionModel struct {
	ID      string
	Type    string
	Payload interface{}
	Stages  []TransactionStageModel
}

// State returns the latest stage for the transaction
func (transaction *TransactionModel) State() TransactionStageModel {
	latestStageOrder := 0
	latestStage := TransactionStageModel{}
	for _, stage := range transaction.Stages {
		if stage.Order > latestStageOrder {
			latestStageOrder = stage.Order
			latestStage = stage
		}
	}
	return latestStage
}

// StateCompleted returns whether the latest step was completed
func (transaction *TransactionModel) StateCompleted(service string) bool {
	state := transaction.State()
	return state.Service == service && state.Ack
}

// AddStage adds a stage to the transaction
func (transaction *TransactionModel) AddStage(stage TransactionStageModel) {
	transaction.Stages = append(transaction.Stages, stage)
}

// HasError returns true if error occured during that stage
func (transactionStage *TransactionStageModel) HasError() bool {
	return transactionStage.Error != nil
}
