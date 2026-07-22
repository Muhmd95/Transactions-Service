package rest

import (
	"svc-transactions/internal/transactions"
)

// TransactionsController handles HTTP requests related to transaction operations.
type TransactionsController struct {
	service *transactions.Service
}

// NewTransactionsController creates a new instance of TransactionsController with the provided wallet service.
func NewTransactionsController(service *transactions.Service) *TransactionsController {
	return &TransactionsController{service: service}
}
