package transactions

import (
	"context"
	"errors"
	"math/rand"
	"svc-transactions/util/logger"
	"time"
)

// this is the rules of the wallet client the service will use
// in the future if i changed the wallet client it should implement the same interface
type WalletClient interface {
	WalletModifyBalance(context.Context, *WalletModifyBalanceRequest) (*WalletModifyBalanceResponse, error)
	GetWalletInfo(ctx context.Context, PhoneNumber string) (*GetWalletResponse, error)
}

type NotificationsClient interface {
	SendSMSNotification(ctx context.Context, req *CreateSMSNotificationRequest) (*CreateSMSNotificationResponse, error)
	SendPushNotification(ctx context.Context, req *CreatePushNotificationRequest) (*CreatePushNotificationResponse, error)
}

// this is a wrapper for the client to handle the transactions
type TxManager interface {
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// for kafka event production
type EventPublisher interface {
	PublishTransactionEvent(ctx context.Context, evt *TransactionEvent) error
}

type Service struct {
	repo                Repository
	walletClient        WalletClient
	notificationsClient NotificationsClient
	txManager           TxManager
	// eventPublisher      EventPublisher
}

func NewService(repo Repository, walletClient WalletClient, txManager TxManager, notificationsClient NotificationsClient) *Service {
	return &Service{repo: repo, walletClient: walletClient, txManager: txManager, notificationsClient: notificationsClient}
}

func (s *Service) CreateDepositTransaction(ctx context.Context, req *DepositRequest, refID string) (*DepositResponse, error) {
	log := logger.Ctx(ctx)

	processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, nil)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			// continue the transaction
		} else {
			log.Error().Err(err).Str("reference_id", refID).Msg("couldn't fetch the transaction by reference id (service layer)")
			return nil, err
		}
	} else {
		return &DepositResponse{
			TransactionID: processedTX.ID,
			WalletID:      processedTX.WalletID,
			Balance:       processedTX.BalanceAfter,
			CreatedAt:     processedTX.CreatedAt,
			Status:        string(processedTX.Status),
		}, nil
	}

	maxRetries := 100

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

			if oldBalance > WalletMax-req.Amount {
				log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
				return ErrExceedsMaxBalance
			}
			newBalance = oldBalance + req.Amount

			NewTransaction = &Transaction{
				Type:          TypeDeposit,
				PhoneNumber:   req.PhoneNumber,
				Amount:        req.Amount,
				Status:        StatusCompleted,
				WalletID:      walletID,
				BalanceBefore: oldBalance,
				BalanceAfter:  newBalance,
				SeqNumber:     newSeqNumber,
				ReferenceID:   refID,

				CreatedAt: time.Now(),
			}

			err = s.repo.CreateTransaction(txCtx, NewTransaction)
			return err
		})

		if err == nil {
			sent = true
			break
		}

		if errors.Is(err, ErrDuplicateReferenceID) {
			processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, nil)
			if err != nil {
				log.Error().Err(err).Str("reference_id", refID).Msg("couldn't fetch the processed transaction by reference id (service layer)")
				return nil, err
			}
			return &DepositResponse{
				TransactionID: processedTX.ID,
				WalletID:      processedTX.WalletID,
				Balance:       processedTX.BalanceAfter,
				CreatedAt:     processedTX.CreatedAt,
				Status:        string(processedTX.Status),
			}, nil
		}

		if errors.Is(err, ErrDuplicateSequence) {
			// some other concurrent request add this transaction first
			// try again and fetch the latest transaction
			// sleep so dont collide at the same time
			// can be caused by refID so i put a check in the start of the iteration
			randomDelay := 5 + rand.Intn(96)
			duration := time.Duration(randomDelay) * time.Millisecond
			time.Sleep(duration)
			continue
		}

		return nil, err
	}

	if !sent {
		log.Error().Msg("Couldn't make the transaction high traffic on this wallet")
		return nil, ErrHighFrequencyTransaction
	}

	// i will handle calling the wallet later
	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      req.Amount,
		RefID:       NewTransaction.ID.Hex(),
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
	log.Info().Msg("Wallet balance has been modified successfully (service layer)")


	// grpc notification sender
	// _, err = s.notificationsClient.SendSMSNotification(ctx, &CreateSMSNotificationRequest{
	// 	PhoneNumber:   req.PhoneNumber,
	// 	Message:       fmt.Sprintf("Your deposit of %d has been successfully processed. Your new balance is %d.", NewTransaction.Amount, newBalance),
	// 	TransactionID: NewTransaction.ID.Hex(),
	// 	WalletID:      walletID,
	// 	Balance:       newBalance,
	// 	Amount:        NewTransaction.Amount,
	// 	CreatedAt:     NewTransaction.CreatedAt,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.PhoneNumber).Msg("Failed to send SMS notification (service layer)")
	// 	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Queuing SMS notification request to NotificationsService (from service layer)")
	// }

	// _, err = s.notificationsClient.SendPushNotification(ctx, &CreatePushNotificationRequest{
	// 	PhoneNumber:   req.PhoneNumber,
	// 	Message:       fmt.Sprintf("Your deposit of %d has been successfully processed. Your new balance is %d.", NewTransaction.Amount, newBalance),
	// 	TransactionID: NewTransaction.ID.Hex(),
	// 	WalletID:      walletID,
	// 	Balance:       newBalance,
	// 	Amount:        NewTransaction.Amount,
	// 	CreatedAt:     NewTransaction.CreatedAt,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.PhoneNumber).Msg("Failed to send push notification (service layer)")
	// 	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Queuing push notification request to NotificationsService (from service layer)")
	// }


	// kafka event eventPublisher
	// evt := &TransactionEvent{
	// 	EventType:    EventWalletCredited,
	// 	TxnID:        NewTransaction.ID.Hex(),
	// 	WalletID:     walletID,
	// 	PhoneNumber:  req.PhoneNumber,
	// 	Amount:       NewTransaction.Amount,
	// 	BalanceAfter: newBalance,
	// 	OccurredAt:   NewTransaction.CreatedAt,
	// 	NationalID:   "", // for the future will wire the user and their wallets
	// }
	// err = s.eventPublisher.PublishTransactionEvent(ctx, evt)
	// if err != nil {
	// 	log.Error().Err(err).Str("wallet_id", walletID).Msg("Failed to publish transaction event to Kafka (service layer)")
	// }
	// log.Info().Msg("Deposit event has been published successfully to Kafka (service layer)")

	return &DepositResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      walletID,
		Balance:       newBalance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil

}

func (s *Service) CreateWithdrawalTransaction(ctx context.Context, req *WithdrawalRequest, refID string) (*WithdrawalResponse, error) {
	maxRetries := 100
	log := logger.Ctx(ctx)

	processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, nil)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			// continue the transaction
		} else {
			log.Error().Err(err).Str("reference_id", refID).Msg("couldn't fetch the transaction by reference id (service layer)")
			return nil, err
		}
	} else {
		return &WithdrawalResponse{
			TransactionID: processedTX.ID,
			WalletID:      processedTX.WalletID,
			Balance:       processedTX.BalanceAfter,
			CreatedAt:     processedTX.CreatedAt,
			Status:        string(processedTX.Status),
		}, nil
	}

	latestTX, err := s.repo.GetLatestTransaction(ctx, req.PhoneNumber)
	if err != nil {
		if errors.Is(err, ErrFirstTransaction) {
			// continue the action
		} else {
			return nil, err
		}
	}

	var walletID string = ""
	if latestTX == nil {
		// check if the wallet exists from the wallet service
		// if it doesnt return that there is no such wallet
		// else continue ur transaction with balance 0
		_, err := s.walletClient.GetWalletInfo(ctx, req.PhoneNumber)
		if err != nil {
			if errors.Is(err, ErrWalletNotFound) {
				log.Error().Err(err).Msg("wallet not found (service layer)")
				return nil, err
			}
			log.Error().Err(err).Msg("couldn't fetch the wallet")
			return nil, err
		}
		return nil, ErrInsufficientBalance // the first transaction of the wallet cant be withdrawal because his init balance is 0
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

			if oldBalance-req.Amount < 0 {
				log.Warn().Err(err).Str("phone_number", req.PhoneNumber).Msg("Insuffienct balance for the withdraw (service layer)")
				return ErrInsufficientBalance
			}
			newBalance = oldBalance - req.Amount

			NewTransaction = &Transaction{
				Type:          TypeWithdrawal,
				PhoneNumber:   req.PhoneNumber,
				Amount:        req.Amount,
				Status:        StatusCompleted,
				WalletID:      walletID,
				BalanceBefore: oldBalance,
				BalanceAfter:  newBalance,
				SeqNumber:     newSeqNumber,
				ReferenceID:   refID,

				CreatedAt: time.Now(),
			}

			err = s.repo.CreateTransaction(txCtx, NewTransaction)
			return err
		})

		if err == nil {
			sent = true
			break
		}

		if errors.Is(err, ErrDuplicateReferenceID) {
			processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, nil)
			if err != nil {
				log.Error().Err(err).Str("reference_id", refID).Msg("couldn't fetch the processed transaction by reference id (service layer)")
				return nil, err
			}
			return &WithdrawalResponse{
				TransactionID: processedTX.ID,
				WalletID:      processedTX.WalletID,
				Balance:       processedTX.BalanceAfter,
				CreatedAt:     processedTX.CreatedAt,
				Status:        string(processedTX.Status),
			}, nil
		}

		if errors.Is(err, ErrDuplicateSequence) {
			// some other concurrent request add this transaction first
			// try again and fetch the latest transaction
			// sleep so dont collide at the same time
			randomDelay := 5 + rand.Intn(96)
			duration := time.Duration(randomDelay) * time.Millisecond
			time.Sleep(duration)
			continue
		}

		return nil, err
	}

	if !sent {
		log.Error().Msg("Couldn't make the transaction high traffic on this wallet")
		return nil, ErrHighFrequencyTransaction
	}

	clientReq := &WalletModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      -req.Amount,
		RefID:       NewTransaction.ID.Hex(),
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
	log.Info().Msg("Wallet balance has been modified successfully (service layer)")


	// grpc notificationsClient
	// _, err = s.notificationsClient.SendSMSNotification(ctx, &CreateSMSNotificationRequest{
	// 	PhoneNumber:   req.PhoneNumber,
	// 	Message:       fmt.Sprintf("Your withdrawal of %d has been successfully processed. Your new balance is %d.", NewTransaction.Amount, newBalance),
	// 	TransactionID: NewTransaction.ID.Hex(),
	// 	WalletID:      walletID,
	// 	Balance:       newBalance,
	// 	Amount:        NewTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.PhoneNumber).Msg("Failed to send SMS notification (service layer)")
	// 	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Queuing SMS notification request to NotificationsService (from service layer)")
	// }

	// _, err = s.notificationsClient.SendPushNotification(ctx, &CreatePushNotificationRequest{
	// 	PhoneNumber:   req.PhoneNumber,
	// 	Message:       fmt.Sprintf("Your withdrawal of %d has been successfully processed. Your new balance is %d.", NewTransaction.Amount, newBalance),
	// 	TransactionID: NewTransaction.ID.Hex(),
	// 	WalletID:      walletID,
	// 	Balance:       newBalance,
	// 	Amount:        NewTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.PhoneNumber).Msg("Failed to send push notification (service layer)")
	// 	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Queuing push notification request to NotificationsService (from service layer)")
	// }


	// kafka event eventPublisher
	// evt := &TransactionEvent{
	// 	EventType:    EventWalletDebited,
	// 	TxnID:        NewTransaction.ID.Hex(),
	// 	PhoneNumber:  req.PhoneNumber,
	// 	WalletID:     walletID,
	// 	Amount:       NewTransaction.Amount,
	// 	BalanceAfter: newBalance,
	// 	OccurredAt:   NewTransaction.CreatedAt,
	// 	NationalID:   "", // for the future will wire the user and their wallets
	// }
	// err = s.eventPublisher.PublishTransactionEvent(ctx, evt)
	// if err != nil {
	// 	log.Error().Err(err).Str("wallet_id", walletID).Msg("Failed to publish transaction event to Kafka (service layer)")
	// }
	// log.Info().Msg("Withdrawal event has been published successfully to Kafka (service layer)")

	return &WithdrawalResponse{
		TransactionID: NewTransaction.ID,
		WalletID:      walletID,
		Balance:       newBalance,
		CreatedAt:     NewTransaction.CreatedAt,
		Status:        string(StatusCompleted),
	}, nil
}

func (s *Service) CreateTransferTransaction(ctx context.Context, req *TransferRequest, refID string) (*TransferResponse, error) {
	maxRetries := 100
	log := logger.Ctx(ctx)

	processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, &req.SenderPhoneNumber)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			// continue the transaction
		} else {
			log.Error().Err(err).Str("reference_id", refID).Msg("couldn't check the transaction by reference id (service layer)")
			return nil, err
		}
	} else {
		if processedTX.Status != StatusCompleted {
			log.Error().Str("reference_id", refID).Msg("Transaction status is not completed, something went wrong")
			return nil, ErrHalfTransferFail
		}
		return &TransferResponse{
			TransactionID:       processedTX.ID,
			SenderWalletID:      processedTX.WalletID,
			Status:              string(processedTX.Status),
			SenderBalanceAfter:  processedTX.BalanceAfter,
			SenderBalanceBefore: processedTX.BalanceBefore,
		}, nil
	}

	if req.SenderPhoneNumber == req.ReceiverPhoneNumber {
		log.Warn().Str("phone_number", req.SenderPhoneNumber).Msg("Sender and receiver phone numbers are the same")
		return nil, ErrInvalidTransactionStatus
	}

	var senderWalletID string = ""
	latestSenderTX, err := s.repo.GetLatestTransaction(ctx, req.SenderPhoneNumber)
	if err != nil {
		if errors.Is(err, ErrFirstTransaction) {
			// the first transaction of the sender cant be transfer because his init balance is 0
			log.Warn().Msg("First transaction of the sender cant be transfer because his init balance is 0")
			return nil, ErrInsufficientBalance
		}
		log.Error().Err(err).Msg("couldn't fetch the latest transaction of the sender")
		return nil, err
	}

	senderWalletID = latestSenderTX.WalletID

	latestReceiverTX, err := s.repo.GetLatestTransaction(ctx, req.ReceiverPhoneNumber)
	if err != nil {
		if errors.Is(err, ErrFirstTransaction) {
			// continue
		} else {
			return nil, err
		}
	}

	var ReceiverWalletID string = ""
	if latestReceiverTX == nil {
		// check if the wallet exists from the wallet service
		// if it doesnt return that there is no such wallet
		// else continue ur transaction with balance 0
		walletRes, err := s.walletClient.GetWalletInfo(ctx, req.ReceiverPhoneNumber)
		if err != nil {
			if errors.Is(err, ErrWalletNotFound) {
				log.Error().Err(err).Msg("wallet not found (service layer)")
				return nil, err
			}
			log.Error().Err(err).Msg("couldn't receiver fetch the wallet")
			return nil, err
		}
		ReceiverWalletID = walletRes.WalletID
	} else {
		ReceiverWalletID = latestReceiverTX.WalletID
	}

	var newWithdrawalTransaction *Transaction
	var newDepositTransaction *Transaction
	var newReceiverBalance int64
	var newSenderBalance int64

	// from here i am sure about the transaction
	var sent bool = false
	var errReason string = ""
	for i := 0; i < maxRetries; i++ {
		err = s.txManager.WithTransaction(ctx, func(txCtx context.Context) error {

			// i am sure that the this is not the first transaction for the sender
			latestSenderTX, err := s.repo.GetLatestTransaction(txCtx, req.SenderPhoneNumber)
			if err != nil {
				return err
			}

			newSenderSeqNumber := latestSenderTX.SeqNumber + 1
			oldSenderBalance := latestSenderTX.BalanceAfter

			if oldSenderBalance-req.Amount < 0 {
				log.Warn().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Insuffienct balance for the withdraw (service layer)")
				return ErrInsufficientBalance
			}
			newSenderBalance = oldSenderBalance - req.Amount

			newWithdrawalTransaction = &Transaction{
				Type:          TypeTransfer,
				PhoneNumber:   req.SenderPhoneNumber,
				Amount:        req.Amount,
				Status:        StatusCompleted,
				SenderPhone:   req.SenderPhoneNumber,
				ReceiverPhone: req.ReceiverPhoneNumber,
				WalletID:      senderWalletID,
				BalanceBefore: oldSenderBalance,
				BalanceAfter:  newSenderBalance,
				SeqNumber:     newSenderSeqNumber,
				ReferenceID:   refID,

				CreatedAt: time.Now(),
			}

			//======================================================================================
			//======================================================================================
			// first part of the transfer is finished the second part is the deposit to the receiver
			//======================================================================================
			//======================================================================================

			latestReceiverTX, err := s.repo.GetLatestTransaction(txCtx, req.ReceiverPhoneNumber)
			if err != nil && !errors.Is(err, ErrFirstTransaction) {
				return err
			}

			var newReceiverSeqNumber int64
			var oldReceiverBalance int64

			if latestReceiverTX == nil {
				newReceiverSeqNumber = 1
				oldReceiverBalance = 0
			} else {
				newReceiverSeqNumber = latestReceiverTX.SeqNumber + 1
				oldReceiverBalance = latestReceiverTX.BalanceAfter
			}

			if oldReceiverBalance > WalletMax-req.Amount {
				log.Warn().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
				return ErrExceedsMaxBalance
			}
			newReceiverBalance = oldReceiverBalance + req.Amount

			newDepositTransaction = &Transaction{
				Type:          TypeTransfer,
				PhoneNumber:   req.ReceiverPhoneNumber,
				Amount:        req.Amount,
				Status:        StatusCompleted,
				SenderPhone:   req.SenderPhoneNumber,
				ReceiverPhone: req.ReceiverPhoneNumber,
				WalletID:      ReceiverWalletID,
				BalanceBefore: oldReceiverBalance,
				BalanceAfter:  newReceiverBalance,
				SeqNumber:     newReceiverSeqNumber,
				ReferenceID:   refID + "_deposit",

				CreatedAt: time.Now(),
			}

			if err := s.repo.CreateCoupledTransaction(txCtx, newDepositTransaction, newWithdrawalTransaction); err != nil {
				return err
			}

			return nil
		})

		if err == nil {
			sent = true
			break
		}

		if errors.Is(err, ErrDuplicateReferenceID) {
			processedTX, err := s.repo.GetTransactionByReferenceID(ctx, refID, nil)
			if err != nil {
				log.Error().Err(err).Str("reference_id", refID).Msg("couldn't fetch the processed transaction by reference id (service layer)")
				return nil, err
			}
			if processedTX.Status != StatusCompleted {
				log.Info().Str("reference_id", refID).Msg("Transaction status is not completed, something went wrong")
				return nil, ErrHalfTransferFail
			}
			return &TransferResponse{
				TransactionID:       processedTX.ID,
				SenderWalletID:      processedTX.WalletID,
				Status:              string(processedTX.Status),
				SenderBalanceAfter:  processedTX.BalanceAfter,
				SenderBalanceBefore: processedTX.BalanceBefore,
			}, nil

		}

		if errors.Is(err, ErrExceedsMaxBalance) {
			errReason = "Exceeds maximum balance"
			break
		}

		if errors.Is(err, ErrDuplicateSequence) {
			// some other concurrent request add this transaction first
			// try again and fetch the latest transaction
			// sleep so dont collide at the same time
			randomDelay := 5 + rand.Intn(96)
			duration := time.Duration(randomDelay) * time.Millisecond
			time.Sleep(duration)
			continue
		}

		return nil, err
	}

	if errReason != "" {
		for i := 0; i < maxRetries; i++ {
			err = s.txManager.WithTransaction(ctx, func(txCtx context.Context) error {
				// i am sure that the sender has a transaction
				latestSenderTX, err := s.repo.GetLatestTransaction(txCtx, req.SenderPhoneNumber)
				if err != nil {
					return err
				}

				newSenderSeqNumber := latestSenderTX.SeqNumber + 1

				newTrasferTransaction := &Transaction{
					Type:          TypeTransfer,
					PhoneNumber:   req.SenderPhoneNumber,
					Amount:        req.Amount,
					Status:        StatusFailed,
					SenderPhone:   req.SenderPhoneNumber,
					ReceiverPhone: req.ReceiverPhoneNumber,
					FailedReason:  "Receiver: " + errReason,
					WalletID:      latestSenderTX.WalletID,
					BalanceBefore: latestSenderTX.BalanceAfter,
					BalanceAfter:  latestSenderTX.BalanceAfter,
					SeqNumber:     newSenderSeqNumber,
					ReferenceID:   refID,

					CreatedAt: time.Now(),
				}

				if err := s.repo.CreateTransaction(txCtx, newTrasferTransaction); err != nil {
					return err
				}

				return nil
			})

			if err == nil {
				sent = true
				break
			}

			if errors.Is(err, ErrDuplicateReferenceID) {
				log.Info().Str("reference_id", refID).Msg("Transaction was already created with failed status")
				return nil, ErrHalfTransferFail
			}

			if errors.Is(err, ErrDuplicateSequence) {
				// some other concurrent request add this transaction first
				// try again and fetch the latest transaction
				// sleep so dont collide at the same time
				randomDelay := 5 + rand.Intn(96)
				duration := time.Duration(randomDelay) * time.Millisecond
				time.Sleep(duration)
				continue
			}
			return nil, err
		}

	}

	// sent will be true if the transaction is successful
	// or if the transaction failed because of the receiver balance
	if !sent {
		log.Error().Msg("Couldn't make the transaction high traffic on this wallet")
		return nil, ErrHighFrequencyTransaction
	}

	if errReason != "" {
		return nil, ErrHalfTransferFail // i return an error but i save the transaction
	}

	clientReqWithdrawal := &WalletModifyBalanceRequest{
		PhoneNumber: req.SenderPhoneNumber,
		Amount:      -req.Amount,
		RefID:       newWithdrawalTransaction.ID.Hex(),
	}

	clientReqDeposit := &WalletModifyBalanceRequest{
		PhoneNumber: req.ReceiverPhoneNumber,
		Amount:      req.Amount,
		RefID:       newDepositTransaction.ID.Hex(),
	}

	// tell the wallet to modify the sender balance
	_, err = s.walletClient.WalletModifyBalance(ctx, clientReqWithdrawal)
	if err != nil { // all these errors wont be done anyway
		if errors.Is(err, ErrWalletNotFound) {
			log.Warn().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Wallet not found (service layer)")
		} else if errors.Is(err, ErrInsufficientBalance) {
			log.Warn().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Insufficient balance (service layer)")
		} else if errors.Is(err, ErrInvalidPhoneNumber) {
			log.Warn().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Invalid phone number format (service layer)")
		}
	}
	log.Info().Msg("Sender wallet balance has been modified successfully (service layer)")

	// tell the wallet to modify the receiver balance
	_, err = s.walletClient.WalletModifyBalance(ctx, clientReqDeposit)
	if err != nil { // all these errors wont be done anyway
		if errors.Is(err, ErrWalletNotFound) {
			log.Warn().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Wallet not found (service layer)")
		} else if errors.Is(err, ErrExceedsMaxBalance) {
			log.Warn().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Deposit exceeds maximum wallet capacity (service layer)")
		} else if errors.Is(err, ErrInvalidPhoneNumber) {
			log.Warn().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Invalid phone number format (service layer)")
		}
	}
	log.Info().Msg("Receiver wallet balance has been modified successfully (service layer)")
	// // notify the sender about the transfer
	// _, err = s.notificationsClient.SendSMSNotification(ctx, &CreateSMSNotificationRequest{
	// 	PhoneNumber:   req.SenderPhoneNumber,
	// 	Message:       fmt.Sprintf("Your transfer of %d to %s has been successfully processed. Your new balance is %d.", req.Amount, req.ReceiverPhoneNumber, newSenderBalance),
	// 	TransactionID: newWithdrawalTransaction.ID.Hex(),
	// 	WalletID:      newWithdrawalTransaction.WalletID,
	// 	Balance:       newSenderBalance,
	// 	Amount:        newWithdrawalTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Failed to send SMS notification (service layer)")
	// 	log.Info().Str("phone_number", req.SenderPhoneNumber).Int64("amount", req.Amount).Msg("Queuing SMS notification request to NotificationsService (from service layer)")
	// }
	// _, err = s.notificationsClient.SendPushNotification(ctx, &CreatePushNotificationRequest{
	// 	PhoneNumber:   req.SenderPhoneNumber,
	// 	Message:       fmt.Sprintf("Your transfer of %d to %s has been successfully processed. Your new balance is %d.", req.Amount, req.ReceiverPhoneNumber, newSenderBalance),
	// 	TransactionID: newWithdrawalTransaction.ID.Hex(),
	// 	WalletID:      newWithdrawalTransaction.WalletID,
	// 	Balance:       newSenderBalance,
	// 	Amount:        newWithdrawalTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.SenderPhoneNumber).Msg("Failed to send push notification (service layer)")
	// 	log.Info().Str("phone_number", req.SenderPhoneNumber).Int64("amount", req.Amount).Msg("Queuing push notification request to NotificationsService (from service layer)")
	// }

	// // notify the receiver about the transfer
	// _, err = s.notificationsClient.SendSMSNotification(ctx, &CreateSMSNotificationRequest{
	// 	PhoneNumber:   req.ReceiverPhoneNumber,
	// 	Message:       fmt.Sprintf("You have received a transfer of %d from %s. Your new balance is %d.", req.Amount, req.SenderPhoneNumber, newReceiverBalance),
	// 	TransactionID: newDepositTransaction.ID.Hex(),
	// 	WalletID:      newDepositTransaction.WalletID,
	// 	Balance:       newReceiverBalance,
	// 	Amount:        newDepositTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Failed to send SMS notification (service layer)")
	// 	log.Info().Str("phone_number", req.ReceiverPhoneNumber).Int64("amount", req.Amount).Msg("Queuing SMS notification request to NotificationsService (from service layer)")
	// }
	// _, err = s.notificationsClient.SendPushNotification(ctx, &CreatePushNotificationRequest{
	// 	PhoneNumber:   req.ReceiverPhoneNumber,
	// 	Message:       fmt.Sprintf("You have received a transfer of %d from %s. Your new balance is %d.", req.Amount, req.SenderPhoneNumber, newReceiverBalance),
	// 	TransactionID: newDepositTransaction.ID.Hex(),
	// 	WalletID:      newDepositTransaction.WalletID,
	// 	Balance:       newReceiverBalance,
	// 	Amount:        newDepositTransaction.Amount,
	// })
	// if err != nil {
	// 	log.Error().Err(err).Str("phone_number", req.ReceiverPhoneNumber).Msg("Failed to send push notification (service layer)")
	// 	log.Info().Str("phone_number", req.ReceiverPhoneNumber).Int64("amount", req.Amount).Msg("Queuing push notification request to NotificationsService (from service layer)")
	// }

	// event for sender
	// evtw := &TransactionEvent{
	// 	EventType:          EventWalletDebited,
	// 	TxnID:              newWithdrawalTransaction.ID.Hex(),
	// 	WalletID:           newWithdrawalTransaction.WalletID,
	// 	PhoneNumber:        req.SenderPhoneNumber,
	// 	Amount:             newWithdrawalTransaction.Amount,
	// 	BalanceAfter:       newSenderBalance,
	// 	OccurredAt:         newWithdrawalTransaction.CreatedAt,
	// 	NationalID:         "", // for the future will wire the user and their wallets
	// 	CoupledPhoneNumber: req.ReceiverPhoneNumber,
	// }
	// err = s.eventPublisher.PublishTransactionEvent(ctx, evtw)
	// if err != nil {
	// 	log.Error().Err(err).Str("wallet_id", newWithdrawalTransaction.WalletID).Msg("Failed to publish transaction event to Kafka (service layer)")
	// }
	// log.Info().Msg("Transfer event for sender has been published successfully to Kafka (service layer)")

	// // event for receiver
	// evtd := &TransactionEvent{
	// 	EventType:          EventWalletCredited,
	// 	TxnID:              newDepositTransaction.ID.Hex(),
	// 	WalletID:           newDepositTransaction.WalletID,
	// 	PhoneNumber:        req.ReceiverPhoneNumber,
	// 	Amount:             newDepositTransaction.Amount,
	// 	BalanceAfter:       newReceiverBalance,
	// 	OccurredAt:         newDepositTransaction.CreatedAt,
	// 	NationalID:         "", // for the future will wire the user and their wallets
	// 	CoupledPhoneNumber: req.SenderPhoneNumber,
	// }
	// err = s.eventPublisher.PublishTransactionEvent(ctx, evtd)
	// if err != nil {
	// 	log.Error().Err(err).Str("wallet_id", newDepositTransaction.WalletID).Msg("Failed to publish transaction event to Kafka (service layer)")
	// }
	// log.Info().Msg("Transfer event for receiver has been published successfully to Kafka (service layer)")	
	
	return &TransferResponse{
		TransactionID:       newWithdrawalTransaction.ID,
		SenderWalletID:      newWithdrawalTransaction.WalletID,
		Status:              string(StatusCompleted),
		SenderBalanceAfter:  newSenderBalance,
		SenderBalanceBefore: newWithdrawalTransaction.BalanceBefore,
	}, nil

}
