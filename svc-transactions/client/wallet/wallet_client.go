package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"svc-transactions/internal/transactions"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"svc-transactions/util/logger"
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
	log := logger.Ctx(ctx)
	bodyBytes, err := json.Marshal(walletReq)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal wallet request (from wallet client)")
		return nil, err
	}

	url := fmt.Sprintf("%s/v1/wallet/balance", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		log.Error().Err(err).Msg("Failed to create HTTP request (from wallet client)")
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send request (from wallet client)")
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		var successResponse transactions.WalletModifyBalanceResponse
		if err := json.NewDecoder(res.Body).Decode(&successResponse); err != nil {
			log.Error().Err(err).Msg("Failed to decode success response (from wallet client)")
			return nil, err
		}
		log.Info().Msg("Successfully modified wallet balance (from wallet client)")
		return &transactions.WalletModifyBalanceResponse{
			WalletID:  successResponse.WalletID,
			Balance:   successResponse.Balance,
			UpdatedAt: successResponse.UpdatedAt,
		}, nil
	}

	var errorResponse transactions.WalletErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errorResponse); err != nil {
		log.Error().Err(err).Msg("Failed to decode error response (from wallet client)")
		return nil, err
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
