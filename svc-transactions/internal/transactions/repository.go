package transactions

import (
	"context"
)

type Repository interface {
	// CreateTransaction creates a new transaction in the database.
	CreateTransaction(ctx context.Context, transaction *Transaction) error
}
