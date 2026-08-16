package transactions

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type DepositRequest struct {
	//ReferenceID string `json:"reference_id"`
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`

	ReferenceID string `json:"reference_id"`
}

type WithdrawalRequest struct {
	//ReferenceID string `json:"reference_id"`
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`

	ReferenceID string `json:"reference_id"`
}

type TransferRequest struct {
	//ReferenceID 	string 	`json:"reference_id"`
	SenderPhoneNumber   	string 	`json:"sender_phone"`
	ReceiverPhoneNumber 	string 	`json:"receiver_phone"`
	Amount 					int64 	`json:"amount"`

	ReferenceID string `json:"reference_id"`
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
	Status  string `json:"status"`
	Balance int64  `json:"balance"`

	CreatedAt time.Time `json:"created_at"`
}

type TransferResponse struct {
	TransactionID 		primitive.ObjectID 	`json:"transaction_id"`
	SenderWalletID      string          	`json:"sender_wallet_id"`
	Status 			 	string 			 	`json:"status"`
	SenderBalanceAfter  int64  				`json:"balance_after"`
	SenderBalanceBefore int64  				`json:"balance_before"`
}

// client DTOs 
type WalletModifyBalanceRequest struct {
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
	RefID  		string 
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
	WalletID string
	OwnerName string
}

type CreateSMSNotificationRequest struct {
	PhoneNumber   string
	Message       string
	TransactionID string
	WalletID      string
	Amount        int64
	Balance       int64
}

type CreateSMSNotificationResponse struct {
	Success        bool
	NotificationID string
}

type CreatePushNotificationRequest struct {
	PhoneNumber   string
	Message       string
	TransactionID string
	WalletID      string
	Amount        int64
	Balance       int64
}
type CreatePushNotificationResponse struct {
	Success        bool
	NotificationID string
}
