package transactions

import (
	"context"
)

type Repository interface {
	// CreateTransaction creates a new transaction in the database.
	CreateTransaction(ctx context.Context, transaction *Transaction) error

	CreateCoupledTransaction(context.Context, *Transaction, *Transaction) error

	GetLatestTransaction(ctx context.Context, PhoneNumber string) (*Transaction, error)

	GetTransactionByReferenceID(ctx context.Context, referenceID string, optionalPhoneNNumber *string) (*Transaction, error)
}
