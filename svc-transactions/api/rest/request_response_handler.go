package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	// paths from project root:
	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)

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

	depositRes, err := c.service.CreateDepositTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrWalletNotFound) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Wallet not found")
			respondWithError(w, http.StatusNotFound, "Wallet not found")
			return
		} else if errors.Is(err, transactions.ErrInsufficientBalance) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Insufficient balance")
			respondWithError(w, http.StatusBadRequest, "Insufficient balance")
			return
		} else if errors.Is(err, transactions.ErrExceedsMaxBalance) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Deposit exceeds maximum wallet capacity")
			respondWithError(w, http.StatusBadRequest, "Deposit exceeds maximum wallet capacity")
			return
		} else if errors.Is(err, transactions.ErrInvalidPhoneNumber) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Invalid phone number")
			respondWithError(w, http.StatusBadRequest, "Invalid phone number")
			return
		}
		log.Error().Err(err).Msg("Failed to modify wallet balance")
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(depositRes); err != nil {
		log.Error().Err(err).Msg("Failed to encode response")
		respondWithError(w, http.StatusInternalServerError, "Failed to encode response")
		return
	}

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Deposit transaction response sent successfully")

}

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

	withdrawRes, err := c.service.CreateWithdrawalTransaction(ctx, &reqData)
	if err != nil {
		if errors.Is(err, transactions.ErrWalletNotFound) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Wallet not found")
			respondWithError(w, http.StatusNotFound, "Wallet not found")
			return
		} else if errors.Is(err, transactions.ErrInsufficientBalance) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Insufficient balance")
			respondWithError(w, http.StatusBadRequest, "Insufficient balance")
			return
		} else if errors.Is(err, transactions.ErrInvalidPhoneNumber) {
			log.Warn().Str("phone_number", reqData.PhoneNumber).Msg("Invalid phone number")
			respondWithError(w, http.StatusBadRequest, "Invalid phone number")
			return
		}
		log.Error().Err(err).Msg("Failed to modify wallet balance")
		respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(withdrawRes); err != nil {
		log.Error().Err(err).Msg("Failed to encode response")
		respondWithError(w, http.StatusInternalServerError, "Failed to encode response")
		return
	}

	log.Info().Str("phone_number", reqData.PhoneNumber).Msg("Withdraw transaction response sent successfully")

}

// helper function to return errors and json response to the client
func respondWithError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
