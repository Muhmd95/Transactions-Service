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

	var reqData transactions.DepositRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		log.Warn().Err(err).Msg(("Invalid request payload"))
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

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
		} else if errors.Is(err, transactions.ErrInsufficientBalance) || errors.Is(err, transactions.ErrExceedsMaxBalance) || errors.Is(err, transactions.ErrInvalidPhoneNumber) {
			respondWithError(w, http.StatusBadRequest, err.Error())
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

	var reqData transactions.WithdrawalRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		log.Warn().Err(err).Msg(("Invalid request payload"))
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if reqData.Amount <= 0 {
		log.Warn().Msg("Withdrawal amount must be greater than zero")
		respondWithError(w, http.StatusBadRequest, "Withdrawal amount must be greater than zero")
		return
	}

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Processing withdrawal transaction request")

	withdrawRes, err := c.service.CreateWithdrawalTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrWalletNotFound) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Wallet not found")
			respondWithError(w, http.StatusNotFound, "Wallet not found")
			return
		} else if errors.Is(err, transactions.ErrInsufficientBalance) || errors.Is(err, transactions.ErrInvalidPhoneNumber) {
			respondWithError(w, http.StatusBadRequest, err.Error())
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

// helper function to return errors and json response to the client
func respondWithError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
