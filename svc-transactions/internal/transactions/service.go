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
	GetWalletInfo(ctx context.Context, PhoneNumber string) (*GetWalletResponse, error)
}

type Service struct {
	repo         Repository
	walletClient WalletClient
	txManager    TxManager
}

func NewService(repo Repository, walletClient WalletClient, txManager TxManager) *Service {
	return &Service{repo: repo, walletClient: walletClient, txManager: txManager}
}

func (s *Service) CreateDepositTransaction(ctx context.Context, req *DepositRequest) (*DepositResponse, error) {
	maxRetries := 10
	log := logger.Ctx(ctx)
	latestTX, err := s.repo.GetLatestTransaction(ctx, req.PhoneNumber)
	if err != nil {
		if errors.Is(err, ErrFirstTransaction) {
		} else {
			return nil, err
		}
	}

	var walletID string = ""
	if latestTX == nil {
		// check if the wallet exists from the wallet service
		// if it doesnt return that there is no such wallet
		// else continue ur transaction with balance 0
		walletRes, err := s.walletClient.GetWalletInfo(ctx, req.PhoneNumber)
		if err != nil {
			if errors.Is(err, ErrWalletNotFound) {
				log.Error().Err(err).Msg("wallet not found (service layer)")
			return nil, err
			}
			log.Error().Err(err).Msg("couldn't fetch the waallet")
			return nil, err
		}
		walletID = walletRes.WalletID
	} else {
		walletID = latestTX.WalletID
	}

	var NewTransaction *Transaction
	var newBalance int64

	// from here i am sure about the transaction
	var sent bool = false
	for i := 0; i < maxRetries; i++ {
		err = s.txManager.WithTransaction(ctx, func(txCtx context.Context) error {
		latestTX, err := s.repo.GetLatestTransaction(txCtx, req.PhoneNumber)
		if err != nil && !errors.Is(err, ErrFirstTransaction) {
			return err
		}

		var newSeqNumber int64
		var oldBalance int64
		
		if latestTX == nil {
			newSeqNumber = 1
			oldBalance = 0
		} else {
			newSeqNumber = latestTX.SeqNumber + 1
			oldBalance = latestTX.BalanceAfter
		}

		if oldBalance > WalletMax - req.Amount {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
			return ErrExceedsMaxBalance
		}
		newBalance = oldBalance + req.Amount

		NewTransaction = &Transaction{
			Type:        	TypeDeposit,
			PhoneNumber: 	req.PhoneNumber,
			Amount:      	req.Amount,
			Status:      	StatusCompleted,
			WalletID:    	walletID,
			BalanceBefore: 	oldBalance,
			BalanceAfter:  	newBalance,
			SeqNumber:     	newSeqNumber,

			CreatedAt:   	time.Now(),
		}

		err = s.repo.CreateTransaction(txCtx, NewTransaction)
		return err
		})

		if err == nil {
			sent = true
			break
		}

		if errors.Is(err, ErrDuplicateSequence) {
			// some other concurrent request add this transaction first
			// try again and fetch the latest transaction
			continue
		}
			
		return nil, err
	}

	if !sent {
		log.Error().Msg("Couldn't make the transaction high traffic on this wallet")
		return nil, err
	}



	// i will handle calling the wallet later
	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      req.Amount,
		RefID: NewTransaction.ID.Hex(),
	}
	// i dont know when to call this
	_, err = s.walletClient.WalletModifyBalance(ctx, clientReq)
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
	}
	// update the transaction to success
	return &DepositResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      walletID,
		Balance:       newBalance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil

	
}

func (s *Service) CreateWithdrawalTransaction(ctx context.Context, req *WithdrawalRequest) (*WithdrawalResponse, error) {
	maxRetries := 10
	log := logger.Ctx(ctx)
	latestTX, err := s.repo.GetLatestTransaction(ctx, req.PhoneNumber)
	if err != nil {
		if errors.Is(err, ErrFirstTransaction) {
		} else {
			return nil, err
		}
	}

	var walletID string = ""
	if latestTX == nil {
		// check if the wallet exists from the wallet service
		// if it doesnt return that there is no such wallet
		// else continue ur transaction with balance 0
		walletRes, err := s.walletClient.GetWalletInfo(ctx, req.PhoneNumber)
		if err != nil {
			if errors.Is(err, ErrWalletNotFound) {
				log.Error().Err(err).Msg("wallet not found (service layer)")
			return nil, err
			}
			log.Error().Err(err).Msg("couldn't fetch the wallet")
			return nil, err
		}
		walletID = walletRes.WalletID
	} else {
		walletID = latestTX.WalletID
	}

	var NewTransaction *Transaction
	var newBalance int64

	// from here i am sure about the transaction
	var sent bool = false
	for i := 0; i < maxRetries; i++ {
		err = s.txManager.WithTransaction(ctx, func(txCtx context.Context) error {
		latestTX, err := s.repo.GetLatestTransaction(txCtx, req.PhoneNumber)
		if err != nil && !errors.Is(err, ErrFirstTransaction) {
			return err
		}

		var newSeqNumber int64
		var oldBalance int64
		
		if latestTX == nil {
			newSeqNumber = 1
			oldBalance = 0
		} else {
			newSeqNumber = latestTX.SeqNumber + 1
			oldBalance = latestTX.BalanceAfter
		}

		if oldBalance - req.Amount < 0 {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Insuffienct balance for the withdraw (service layer)")
			return ErrInsufficientBalance
		}
		newBalance = oldBalance - req.Amount

		NewTransaction = &Transaction{
			Type:        	TypeWithdrawal,
			PhoneNumber: 	req.PhoneNumber,
			Amount:      	req.Amount,
			Status:      	StatusCompleted,
			WalletID:    	walletID,
			BalanceBefore: 	oldBalance,
			BalanceAfter:  	newBalance,
			SeqNumber:     	newSeqNumber,

			CreatedAt:   	time.Now(),
		}

		err = s.repo.CreateTransaction(txCtx, NewTransaction)
		return err
		})

		if err == nil {
			sent = true
			break
		}

		if errors.Is(err, ErrDuplicateSequence) {
			// some other concurrent request add this transaction first
			// try again and fetch the latest transaction
			continue
		}
			
		return nil, err
	}

	if !sent {
		log.Error().Msg("Couldn't make the transaction high traffic on this wallet")
		return nil, err
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: 	req.PhoneNumber,
		Amount:      	-req.Amount,
		RefID: 			NewTransaction.ID.Hex(),
	}

	_, err = s.walletClient.WalletModifyBalance(ctx, clientReq)
	if err != nil {
		if errors.Is(err, ErrWalletNotFound) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Wallet not found (service layer)")
		} else if errors.Is(err, ErrInsufficientBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Insufficient balance (service layer)")
		} else if errors.Is(err, ErrExceedsMaxBalance) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
		} else if errors.Is(err, ErrInvalidPhoneNumber) {
			log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Invalid phone number format (service layer)")
		}
	}

	return &WithdrawalResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      walletID,
		Balance:       newBalance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil
}
