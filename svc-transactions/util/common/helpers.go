package common

import (
	"strings"
	"svc-transactions/internal/transactions"
)

func ValidatePhoneNumber(phoneNumber *string) error {
	*phoneNumber = strings.TrimSpace(*phoneNumber)
	if !strings.HasPrefix(*phoneNumber, "+20") || len(*phoneNumber) != 13 {
		return transactions.ErrInvalidPhoneNumber // return the domain error for invalid phone number format
	}

	// 2. Verify the payload is numeric (prevents +20ABCDEFGHIJ)
	for _, ch := range (*phoneNumber)[3:] {
		if ch < '0' || ch > '9' {
			return transactions.ErrInvalidPhoneNumber // return the domain error for invalid phone number format
		}
	}
	return nil
}