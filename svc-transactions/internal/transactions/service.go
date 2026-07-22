package transactions

import (
	"context"
)

// this is the rules of the wallet client the service will use
// in the future if i changed the wallet client it should implement the same interface
type WalletClient interface {
	ModifyBalance(ctx context.Context, walletReq *WalletModifyBalanceRequest) (*WalletModifyBalanceResponse, error)
}

type Service struct {
	repo         Repository
	walletClient WalletClient
}

func NewService(repo Repository, walletClient WalletClient) *Service {
	return &Service{repo: repo, walletClient: walletClient}
}

func (s *Service) CreateTransaction(ctx context.Context, transaction *Transaction) (int64, error) {}
