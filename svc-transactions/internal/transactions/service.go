package transactions

import (
	"context"
	"time"
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

func (s *Service) CreateDepositTransaction(ctx context.Context, req *DepositRequest) (*DepositResponse, error) {

	NewTransaction := &Transaction{
		Type:          TypeDeposit,
		ReceiverPhone: req.PhoneNumber,
		Amount:        req.Amount,
		Status:        StatusPending,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, NewTransaction); err != nil {
		// i dont know what to do with this error yet
		return nil, err
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      req.Amount,
	}

	clientRes, err := s.walletClient.ModifyBalance(ctx, clientReq)
	if err != nil {
		// update the transaction to failed
		s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusFailed, err.Error())
		// i dont know what to do with this error yet
		return nil, err
	}
	// update the transaction to success
	s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusCompleted, "")

	return &DepositResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      clientRes.WalletID,
		Balance:       clientRes.Balance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil
}

func (s *Service) CreateWithdrawalTransaction(ctx context.Context, req *WithdrawalRequest) (*WithdrawalResponse, error) {

	NewTransaction := &Transaction{
		Type:        TypeWithdrawal,
		SenderPhone: req.PhoneNumber,
		Amount:      req.Amount,
		Status:      StatusPending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, NewTransaction); err != nil {
		// i dont know what to do with this error yet
		return nil, err
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      -req.Amount,
	}

	clientRes, err := s.walletClient.ModifyBalance(ctx, clientReq)
	if err != nil {
		// update the transaction to failed
		s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusFailed, err.Error())
		// i dont know what to do with this error yet
		return nil, err
	}
	// update the transaction to success
	s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusCompleted, "")

	return &WithdrawalResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      clientRes.WalletID,
		Balance:       clientRes.Balance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil
}
