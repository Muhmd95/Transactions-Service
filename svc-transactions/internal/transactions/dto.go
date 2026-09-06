package transactions

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type DepositRequest struct {
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
}

type WithdrawalRequest struct {
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
}

type TransferRequest struct {
	SenderPhoneNumber   string `json:"sender_phone"`
	ReceiverPhoneNumber string `json:"receiver_phone"`
	Amount              int64  `json:"amount"`
}

type DepositResponse struct {
	TransactionID primitive.ObjectID `json:"transaction_id"`
	WalletID      string             `json:"wallet_id"`

	Status  string `json:"status"`
	Balance int64  `json:"balance"`

	CreatedAt time.Time `json:"created_at"`
}

type WithdrawalResponse struct {
	TransactionID primitive.ObjectID `json:"transaction_id"`
	WalletID      string             `json:"wallet_id"`
	Status        string             `json:"status"`
	Balance       int64              `json:"balance"`

	CreatedAt time.Time `json:"created_at"`
}

type TransferResponse struct {
	TransactionID       primitive.ObjectID `json:"transaction_id"`
	SenderWalletID      string             `json:"sender_wallet_id"`
	Status              string             `json:"status"`
	SenderBalanceAfter  int64              `json:"balance_after"`
	SenderBalanceBefore int64              `json:"balance_before"`
}

// wallet client dtos
type WalletModifyBalanceRequest struct {
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
	RefID       string
}

type WalletModifyBalanceResponse struct {
	WalletID string `json:"wallet_id"`
	Balance  int64  `json:"balance"`
	// currency code will be added in the future
	UpdatedAt time.Time `json:"updated_at"`
}

type WalletErrorResponse struct {
	Error string `json:"error"`
}

type GetWalletResponse struct {
	WalletID  string
	OwnerName string
}

// notifications client dtos phase 3
// type CreateSMSNotificationRequest struct {
// 	PhoneNumber   string
// 	Message       string
// 	TransactionID string
// 	WalletID      string
// 	Amount        int64
// 	Balance       int64
// 	CreatedAt     time.Time
// }

// type CreateSMSNotificationResponse struct {
// 	Success        bool
// 	NotificationID string
// }

// type CreatePushNotificationRequest struct {
// 	PhoneNumber   string
// 	Message       string
// 	TransactionID string
// 	WalletID      string
// 	Amount        int64
// 	Balance       int64
// 	CreatedAt     time.Time
// }
// type CreatePushNotificationResponse struct {
// 	Success        bool
// 	NotificationID string
// }

// kafka phase 4 dtos
// const (
// 	EventWalletCredited = "WALLET_CREDITED"
// 	EventWalletDebited  = "WALLET_DEBITED"
// )

// type TransactionEvent struct {
// 	EventType          string // "WALLET_CREDITED" | "WALLET_DEBITED"
// 	TxnID              string
// 	WalletID           string // partition key
// 	PhoneNumber        string
// 	NationalID         string // for the future will wire the user and their wallets
// 	Amount             int64  // amount of the txn
// 	BalanceAfter       int64
// 	OccurredAt         time.Time // business time when the money moved
// 	CoupledPhoneNumber string    // when the transaction is transfer will be put with the sender						// must be checked first in the notifications service
// }
