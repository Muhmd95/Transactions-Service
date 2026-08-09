package transactions

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type TransactionType string

const (
	TypeDeposit    TransactionType = "DEPOSIT"
	TypeWithdrawal TransactionType = "WITHDRAWAL"
	TypeTransfer   TransactionType = "TRANSFER"
)

type TransactionStatus string

const (
	StatusCompleted TransactionStatus = "POSTED"
	StatusFailed    TransactionStatus = "FAILED"
)

type Transaction struct {
	ID primitive.ObjectID `bson:"_id,omitempty"`

	//referece id is a string to prevent idempotency
	//ReferenceID string `bson:"reference_id"`
	PhoneNumber string 	 `bson:"phone_number"`

	// empty if the transaction is deposit or withdraw
	SenderPhone   string `bson:"sender_phone,omitEmpty"` 
	ReceiverPhone string `bson:"receiver_phone,omitEmpty"`

	Type   TransactionType   `bson:"type"`
	Status TransactionStatus `bson:"status"`

	Amount int64 `bson:"amount"`

	//data of the wallet
	WalletID      string `bson:"wallet_id"`
	BalanceBefore int64  `bson:"balance_before"`
	BalanceAfter  int64  `bson:"balance_after"`
	SeqNumber     int64  `bson:"sequence_number"`
	
	//CurrencyCode string `bson:"currency_code"`

	FailedReason string    `bson:"failed_reason,omitempty"`
	CreatedAt    time.Time `bson:"created_at"`
}

// --- Domain Errors ---
// The service layer will check for these exact errors without knowing about
// MongoDB to completely separate service from db
var (

	// errors related to wallet operations
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrInvalidPhoneNumber  = errors.New("invalid phone number format")
	ErrInsufficientBalance = errors.New("insufficient balance for the requested operation")
	ErrExceedsMaxBalance   = errors.New("deposit exceeds maximum wallet capacity")
	// errors related  to transaction
	//ErrDuplicateReferenceID   = errors.New("transaction with the same reference ID already exists")
	ErrInvalidTransactionType = errors.New("invalid transaction type")
	ErrFirstTransaction = errors.New("first transaction insertion")
	ErrDuplicateSequence = errors.New("duplicate sequence number")
)

// wallet max
const WalletMax int64 = 9000000000000000

