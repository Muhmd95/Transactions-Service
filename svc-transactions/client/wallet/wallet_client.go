package wallet

import (
	"context"
	"fmt"

	walletv1 "github.com/Muhmd95/Contracts/wallet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)



type grpcClient struct {
	client walletv1.WalletServiceClient
}

func NewWalletClient(conn grpc.ClientConnInterface) transactions.WalletClient {
	return &grpcClient{
		client: walletv1.NewWalletServiceClient(conn),
	}

}

func (c *grpcClient) WalletModifyBalance(ctx context.Context, req *transactions.WalletModifyBalanceRequest) (*transactions.WalletModifyBalanceResponse, error) {
	log := logger.Ctx(ctx)
	walletReq := &walletv1.ModifyBalanceRequest{
		PhoneNumber: req.PhoneNumber,
		Amount:      req.Amount,
	}

	log.Info().Str("phone_number", walletReq.PhoneNumber).Int64("amount", walletReq.Amount).Msg("Sending ModifyBalance request to WalletService (from grpc wallet client)")

	clientRes, err := c.client.ModifyBalance(ctx, walletReq)
	if err != nil {
		st, ok := status.FromError(err)
		// ok will be true only if the error is from a grpc server
		if !ok {
			log.Error().Err(err).Msg("Failed to call ModifyBalance (from grpc wallet client)")
			return nil, fmt.Errorf("failed to call ModifyBalance: %w", err)
		}
		switch st.Code() {
		case codes.InvalidArgument:
			return nil, transactions.ErrInvalidPhoneNumber
		case codes.NotFound:
			return nil, transactions.ErrWalletNotFound
		case codes.FailedPrecondition:
			if st.Message() == transactions.ErrInsufficientBalance.Error() {
				return nil, transactions.ErrInsufficientBalance
			} else if st.Message() == transactions.ErrExceedsMaxBalance.Error() {
				return nil, transactions.ErrExceedsMaxBalance
			}
		case codes.Internal: 
			log.Error().Err(err).Msg("Internal server error from WalletService (from grpc wallet client)")
			return nil, fmt.Errorf("internal server error from WalletService: %w", err)
		default:
			log.Error().Err(err).Msg("Unexpected error from WalletService (from grpc wallet client)")
			return nil, fmt.Errorf("unexpected error from WalletService: %w", err)
		}
	}

	log.Info().Str("phone_number", walletReq.PhoneNumber).Int64("amount", walletReq.Amount).Msg("Successfully modified wallet balance (from grpc wallet client)")
	return &transactions.WalletModifyBalanceResponse{
		WalletID:  clientRes.GetWalletId(),
		Balance:   clientRes.GetBalance(),
		UpdatedAt: clientRes.GetUpdatedAt().AsTime(),
	}, nil
}


// type httpClient struct {
// 	baseURL string
// 	client  *http.Client
// }

// func NewWalletClient(baseURL string) transactions.WalletClient {
// 	return &httpClient{
// 		baseURL: baseURL,
// 		client:  &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}, // need explaiantion
// 	}

// }

// func (c *httpClient) ModifyBalance(ctx context.Context, walletReq *transactions.WalletModifyBalanceRequest) (*transactions.WalletModifyBalanceResponse, error) {
// 	log := logger.Ctx(ctx)
// 	bodyBytes, err := json.Marshal(walletReq)
// 	if err != nil {
// 		log.Error().Err(err).Msg("Failed to marshal wallet request (from wallet client)")
// 		return nil, err
// 	}

// 	url := fmt.Sprintf("%s/v1/wallet/balance", c.baseURL)
// 	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewBuffer(bodyBytes))
// 	if err != nil {
// 		log.Error().Err(err).Msg("Failed to create HTTP request (from wallet client)")
// 		return nil, err
// 	}

// 	res, err := c.client.Do(req)
// 	if err != nil {
// 		log.Error().Err(err).Msg("Failed to send request (from wallet client)")
// 		return nil, fmt.Errorf("failed to send request: %w", err)
// 	}
// 	defer res.Body.Close()

// 	if res.StatusCode == http.StatusOK {
// 		var successResponse transactions.WalletModifyBalanceResponse
// 		if err := json.NewDecoder(res.Body).Decode(&successResponse); err != nil {
// 			log.Error().Err(err).Msg("Failed to decode success response (from wallet client)")
// 			return nil, err
// 		}
// 		log.Info().Msg("Successfully modified wallet balance (from wallet client)")
// 		return &transactions.WalletModifyBalanceResponse{
// 			WalletID:  successResponse.WalletID,
// 			Balance:   successResponse.Balance,
// 			UpdatedAt: successResponse.UpdatedAt,
// 		}, nil
// 	}

// 	var errorResponse transactions.WalletErrorResponse
// 	if err := json.NewDecoder(res.Body).Decode(&errorResponse); err != nil {
// 		log.Error().Err(err).Msg("Failed to decode error response (from wallet client)")
// 		return nil, err
// 	}

// 	// Map the exact string returned by the wallet service back to the domain errors
// 	switch errorResponse.Error {
// 	case "wallet not found":
// 		return nil, transactions.ErrWalletNotFound
// 	case "invalid phone number format":
// 		return nil, transactions.ErrInvalidPhoneNumber
// 	case "insufficient balance for the requested operation":
// 		return nil, transactions.ErrInsufficientBalance
// 	case "deposit exceeds maximum wallet capacity":
// 		return nil, transactions.ErrExceedsMaxBalance
// 	default:
// 		// Fallback for any unknown errors
// 		return nil, fmt.Errorf("wallet service error: %s", errorResponse.Error)
// 	}
// }
