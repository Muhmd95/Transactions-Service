package transactions

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Repository interface {
	// CreateTransaction creates a new transaction in the database.
	CreateTransaction(ctx context.Context, transaction *Transaction) error

	UpdateTransactionStatus(ctx context.Context, transactionID primitive.ObjectID, status TransactionStatus, failedReason string) error
}
