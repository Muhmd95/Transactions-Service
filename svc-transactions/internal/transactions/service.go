package transactions

import (
	"context"
	"time"
	"svc-transactions/util/logger"
	"errors"
)

// this is the rules of the wallet client the service will use
// in the future if i changed the wallet client it should implement the same interface
type WalletClient interface {
	WalletModifyBalance(context.Context, *WalletModifyBalanceRequest) (*WalletModifyBalanceResponse, error)
}

type Service struct {
	repo         Repository
	walletClient WalletClient
}

func NewService(repo Repository, walletClient WalletClient) *Service {
	return &Service{repo: repo, walletClient: walletClient}
}

func (s *Service) CreateDepositTransaction(ctx context.Context, req *DepositRequest) (*DepositResponse, error) {

	log := logger.Ctx(ctx)
	NewTransaction := &Transaction{
		Type:          TypeDeposit,
		ReceiverPhone: req.PhoneNumber,
		Amount:        req.Amount,
		Status:        StatusPending,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, NewTransaction); err != nil {
		return nil, err
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      req.Amount,
	}

	clientRes, err := s.walletClient.WalletModifyBalance(ctx, clientReq)
	if err != nil {
		// update the transaction to failed
		if errors.Is(err, ErrWalletNotFound) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Wallet not found (service layer)")
		} else if errors.Is(err, ErrInsufficientBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Insufficient balance (service layer)")
		} else if errors.Is(err, ErrExceedsMaxBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
		} else if errors.Is(err, ErrInvalidPhoneNumber) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Invalid phone number format (service layer)")
		}
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

	log := logger.Ctx(ctx)
	NewTransaction := &Transaction{
		Type:        TypeWithdrawal,
		SenderPhone: req.PhoneNumber,
		Amount:      req.Amount,
		Status:      StatusPending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, NewTransaction); err != nil {
		return nil, err
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      -req.Amount,
	}

	clientRes, err := s.walletClient.WalletModifyBalance(ctx, clientReq)
	if err != nil {
		// update the transaction to failed
		if errors.Is(err, ErrWalletNotFound) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Wallet not found (service layer)")
		} else if errors.Is(err, ErrInsufficientBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Insufficient balance (service layer)")
		} else if errors.Is(err, ErrExceedsMaxBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
		} else if errors.Is(err, ErrInvalidPhoneNumber) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Invalid phone number format (service layer)")
		}
		err = s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusFailed, err.Error())
		if err != nil {
			log.Error().Err(err).Str("transaction_id", NewTransaction.ID.Hex()).Msg("Failed to update transaction status to failed (service layer)")
		}
		// i dont know what to do with this error yet
		return nil, err
	}
	// update the transaction to success
	err = s.repo.UpdateTransactionStatus(ctx, NewTransaction.ID, StatusCompleted, "")
	if err != nil {
		log.Error().Err(err).Str("transaction_id", NewTransaction.ID.Hex()).Msg("Failed to update transaction status to completed (service layer)")
		return nil, err
	}

	return &WithdrawalResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      clientRes.WalletID,
		Balance:       clientRes.Balance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil
}
