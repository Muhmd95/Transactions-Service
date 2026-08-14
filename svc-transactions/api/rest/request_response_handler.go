package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	// paths from project root:
	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
	"svc-transactions/util/common"
)

// DepositHandler handles processing deposit transactions.
// @Summary      Process a deposit transaction
// @Description  Validates the incoming payload, checks wallet constraints, and credits the wallet balance.
// @Tags         Transactions
// @Accept       json
// @Produce      json
// @Param        request  body      transactions.DepositRequest  true  "Deposit Request Payload"
// @Param        Idempotency-Key  header    string  true  "Unique key to prevent duplicate balance modifications"
// @Success      201      {object}  transactions.DepositResponse "Deposit transaction successful & created"
// @Failure      400      {object}  map[string]string            "Bad Request (Invalid payload, insufficient balance, capacity limit, or phone number)"
// @Failure      404      {object}  map[string]string            "Wallet Not Found"
// @Failure      405      {object}  map[string]string            "Method Not Allowed"
// @Failure      500      {object}  map[string]string            "Internal Server Error"
// @Router       /transactions/deposit [post]
func (c *TransactionsController) DepositHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()
	log := logger.Ctx(ctx)
	// handle the deposit request
	if r.Method != http.MethodPost {
		log.Warn().Msg("Method not allowed")
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	refID := r.Header.Get("Idempotency-Key")
	if refID == "" {
		log.Warn().Msg("Missing Idempotency-Key header")
		respondWithError(w, http.StatusBadRequest, "Missing Idempotency-Key header")
		return
	}

	var reqData transactions.DepositRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		log.Warn().Err(err).Msg(("Invalid request payload"))
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	reqData.ReferenceID = refID

	if reqData.Amount <= 0 {
		log.Warn().Msg("Deposit amount must be greater than zero")
		respondWithError(w, http.StatusBadRequest, "Deposit amount must be greater than zero")
		return
	}

	// validate the phone number before talking to the wallet
	if err := common.ValidatePhoneNumber(&reqData.PhoneNumber); err != nil {
		log.Warn().Err(err).Str("phone_number", reqData.PhoneNumber).Msg("Invalid phone number format")
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Processing deposit transaction request")

	depositRes, err := c.service.CreateDepositTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrWalletNotFound) {
			respondWithError(w, http.StatusNotFound, "Wallet not found")
			return
		} else if errors.Is(err, transactions.ErrExceedsMaxBalance) {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		} else if errors.Is(err, transactions.ErrHighFrequencyTransaction) {
			respondWithError(w, http.StatusTooManyRequests, err.Error())
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	responseData, err := json.Marshal(depositRes)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal response")
		respondWithError(w, http.StatusInternalServerError, "Failed to marshal response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(responseData)

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Deposit transaction response sent successfully")

}

// WithdrawHandler handles processing withdrawal transactions.
// @Summary      Process a withdrawal transaction
// @Description  Validates the incoming payload, checks wallet balance, and debits the wallet balance.
// @Tags         Transactions
// @Accept       json
// @Produce      json
// @Param        request  body      transactions.WithdrawalRequest  true  "Withdrawal Request Payload"
// @Param        Idempotency-Key  header    string  true  "Unique key to prevent duplicate balance modifications"
// @Success      201      {object}  transactions.WithdrawalResponse "withdrawal transaction successful & created"
// @Failure      400      {object}  map[string]string               "Bad Request (Invalid payload, insufficient balance, or phone number)"
// @Failure      404      {object}  map[string]string               "Wallet Not Found"
// @Failure      405      {object}  map[string]string               "Method Not Allowed"
// @Failure      500      {object}  map[string]string               "Internal Server Error"
// @Router       /transactions/withdraw [post]
func (c *TransactionsController) WithdrawHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()
	log := logger.Ctx(ctx)
	// handle the withdraw request
	if r.Method != http.MethodPost {
		log.Warn().Msg("Method not allowed")
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	refID := r.Header.Get("Idempotency-Key")
	if refID == "" {
		log.Warn().Msg("Missing Idempotency-Key header")
		respondWithError(w, http.StatusBadRequest, "Missing Idempotency-Key header")
		return
	}



	var reqData transactions.WithdrawalRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		log.Warn().Err(err).Msg(("Invalid request payload"))
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	reqData.ReferenceID = refID

	if reqData.Amount <= 0 {
		log.Warn().Msg("Withdrawal amount must be greater than zero")
		respondWithError(w, http.StatusBadRequest, "Withdrawal amount must be greater than zero")
		return
	}

	// validate the phone number before talking to the wallet
	if err := common.ValidatePhoneNumber(&reqData.PhoneNumber); err != nil {
		log.Warn().Err(err).Str("phone_number", reqData.PhoneNumber).Msg("Invalid phone number format")
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Processing withdrawal transaction request")

	withdrawRes, err := c.service.CreateWithdrawalTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrWalletNotFound) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Wallet not found")
			respondWithError(w, http.StatusNotFound, "Wallet not found")
			return
		} else if errors.Is(err, transactions.ErrInsufficientBalance) {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		} else if errors.Is(err, transactions.ErrHighFrequencyTransaction) {
			respondWithError(w, http.StatusTooManyRequests, err.Error())
			return
		}	
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	responseData, err := json.Marshal(withdrawRes)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal response")
		respondWithError(w, http.StatusInternalServerError, "Failed to marshal response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(responseData)

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Withdraw transaction response sent successfully")

}

// TransferHandler handles processing transfer transactions between two wallets.
// @Summary      Process a transfer transaction
// @Description  Validates the incoming payload, checks sender balance, and atomically transfers funds from sender to receiver.
// @Tags         Transactions
// @Accept       json
// @Produce      json
// @Param        request  body      transactions.TransferRequest  true  "Transfer Request Payload"
// @Param        Idempotency-Key  header    string  true  "Unique key to prevent duplicate transfers"
// @Success      201      {object}  transactions.TransferResponse "Transfer transaction successful & created"
// @Failure      400      {object}  map[string]string               "Bad Request (Invalid payload, insufficient balance, or phone number)"
// @Failure      404      {object}  map[string]string               "Sender or Receiver Wallet Not Found"
// @Failure      405      {object}  map[string]string               "Method Not Allowed"
// @Failure      422      {object}  map[string]string               "Unprocessable Entity (Transfer failed due to receiver max capacity)"
// @Failure      429      {object}  map[string]string               "Too Many Requests (Concurrency throttle)"
// @Failure      500      {object}  map[string]string               "Internal Server Error"
// @Router       /transactions/transfer [post]
func (c *TransactionsController) TransferHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()
	log := logger.Ctx(ctx)
	// handle the withdraw request
	if r.Method != http.MethodPost {
		log.Warn().Msg("Method not allowed")
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	refID := r.Header.Get("Idempotency-Key")
	if refID == "" {
		log.Warn().Msg("Missing Idempotency-Key header")
		respondWithError(w, http.StatusBadRequest, "Missing Idempotency-Key header")
		return
	}

	var reqData transactions.TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		log.Warn().Err(err).Msg(("Invalid request payload"))
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	reqData.ReferenceID = refID

	if reqData.Amount <= 0 {
		log.Warn().Msg("Transfer amount must be greater than zero")
		respondWithError(w, http.StatusBadRequest, "Transfer amount must be greater than zero")
		return
	}

	// validate the sender phone number before talking to the wallet
	if err := common.ValidatePhoneNumber(&reqData.SenderPhoneNumber); err != nil {
		log.Warn().Err(err).Str("phone_number", reqData.SenderPhoneNumber).Msg("Invalid sender phone number format")
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	// validate the receiver phone number before talking to the wallet
	if err := common.ValidatePhoneNumber(&reqData.ReceiverPhoneNumber); err != nil {
		log.Warn().Err(err).Str("phone_number", reqData.ReceiverPhoneNumber).Msg("Invalid receiver phone number format")
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Info().Msg("Processing transfer transaction request")

	transferRes, err := c.service.CreateTransferTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrInvalidTransactionStatus) {
			log.Warn().Msg("Invalid transaction status")
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, transactions.ErrWalletNotFound) {
			log.Warn().Msg("Wallet not found")
			respondWithError(w, http.StatusNotFound, "Sender or Receiver wallet not found")
			return
		} else if errors.Is(err, transactions.ErrInsufficientBalance) || errors.Is(err, transactions.ErrExceedsMaxBalance) {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		} else if errors.Is(err, transactions.ErrHighFrequencyTransaction) {
			respondWithError(w, http.StatusTooManyRequests, err.Error())
			return
		} else if errors.Is(err, transactions.ErrHalfTransferFail) {
			respondWithError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	responseData, err := json.Marshal(transferRes)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal response")
		respondWithError(w, http.StatusInternalServerError, "Failed to marshal response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(responseData)

	log.Info().Msg("Transfer transaction response sent successfully")

}

// helper function to return errors and json response to the client
func respondWithError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
