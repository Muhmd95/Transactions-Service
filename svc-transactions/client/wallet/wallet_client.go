package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"svc-transactions/internal/transactions"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type httpClient struct {
	baseURL string
	client  *http.Client
}

func NewWalletClient(baseURL string) transactions.WalletClient {
	return &httpClient{
		baseURL: baseURL,
		client:  &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}, // need explaiantion
	}

}

func (c *httpClient) ModifyBalance(ctx context.Context, walletReq *transactions.WalletModifyBalanceRequest) (*transactions.WalletModifyBalanceResponse, error) {

	bodyBytes, err := json.Marshal(walletReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	url := fmt.Sprintf("%s/v1/wallets/balance", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		var successResponse transactions.WalletModifyBalanceResponse
		if err := json.NewDecoder(res.Body).Decode(&successResponse); err != nil {
			return nil, fmt.Errorf("failed to decode success response: %w", err)
		}
		return &transactions.WalletModifyBalanceResponse{
			WalletID:  successResponse.WalletID,
			Balance:   successResponse.Balance,
			UpdatedAt: successResponse.UpdatedAt,
		}, nil
	}

	var errorResponse transactions.WalletErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errorResponse); err != nil {
		return nil, fmt.Errorf("failed to decode error response: %w", err)
	}

	// Map the exact string returned by the wallet service back to the domain errors
	switch errorResponse.Error {
	case "wallet not found":
		return nil, transactions.ErrWalletNotFound
	case "invalid phone number format":
		return nil, transactions.ErrInvalidPhoneNumber
	case "insufficient balance for the requested operation":
		return nil, transactions.ErrInsufficientBalance
	case "deposit exceeds maximum wallet capacity":
		return nil, transactions.ErrExceedsMaxBalance
	default:
		// Fallback for any unknown errors
		return nil, fmt.Errorf("wallet service error: %s", errorResponse.Error)
	}
}
