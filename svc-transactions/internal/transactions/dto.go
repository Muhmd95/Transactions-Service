package transactions

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type DepositRequest struct {
	//ReferenceID string `json:"reference_id"`
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
}

type WithdrawalRequest struct {
	//ReferenceID string `json:"reference_id"`
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
}

type DepositResponse struct {
	TransactionID primitive.ObjectID `json:"transaction_id"`
	WalletID      string             `json:"wallet_id"`
	//ReferenceID   string             `json:"reference_id"`
	Status  string `json:"status"`
	Balance int64  `json:"balance"`

	CreatedAt time.Time `json:"created_at"`
}

type WithdrawalResponse struct {
	TransactionID primitive.ObjectID `json:"transaction_id"`
	WalletID      string             `json:"wallet_id"`
	//ReferenceID   string             `json:"reference_id"`
	Status  string `json:"status"`
	Balance int64  `json:"balance"`

	CreatedAt time.Time `json:"created_at"`
}

// client DTOs (temporary)
type WalletModifyBalanceRequest struct {
	PhoneNumber string `json:"phone_number"`
	Amount      int64  `json:"amount"`
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
