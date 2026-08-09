package transactions


import (
	"context"
)

type TxManager interface {
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error ) error
}