package common

import (
	"strings"
	"svc-transactions/internal/transactions"
)

func ValidatePhoneNumber(phoneNumber *string) error {
	*phoneNumber = strings.TrimSpace(*phoneNumber)
	if !(strings.HasPrefix(*phoneNumber, "010") || strings.HasPrefix(*phoneNumber, "011") || strings.HasPrefix(*phoneNumber, "012") || strings.HasPrefix(*phoneNumber, "015")) || len(*phoneNumber) != 11 {
		return transactions.ErrInvalidPhoneNumber // return the domain error for invalid phone number format
	}

	// 2. Verify the payload is numeric (prevents +20ABCDEFGHIJ)
	for _, ch := range (*phoneNumber) {
		if ch < '0' || ch > '9' {
			return transactions.ErrInvalidPhoneNumber // return the domain error for invalid phone number format
		}
	}
	return nil
}
