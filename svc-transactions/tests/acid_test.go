//go:build integration

package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	WalletSvcURL = "http://localhost:8000"
	TxSvcURL     = "http://localhost:8080"
)

var httpClient = &http.Client{Timeout: 120 * time.Second}

func createWallet(t *testing.T, phone, name, nationalID string) string {
	payload := map[string]interface{}{
		"owner_name":    name,
		"phone_number":  phone,
		"currency_code": "EGP",
		"national_id":   nationalID,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, WalletSvcURL+"/v1/wallet", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create wallet: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("Unexpected status creating wallet: %d, body: %s", resp.StatusCode, string(respBody))
	}

	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	if wID, ok := data["wallet_id"].(string); ok {
		return wID
	}
	// Try to fetch it if it already exists
	if resp.StatusCode == http.StatusConflict {
		req, _ = http.NewRequest(http.MethodGet, WalletSvcURL+"/v1/wallet/phone/"+phone, nil)
		resp2, err := httpClient.Do(req)
		if err == nil {
			defer resp2.Body.Close()
			b2, _ := io.ReadAll(resp2.Body)
			var d2 map[string]interface{}
			json.Unmarshal(b2, &d2)
			if w, ok := d2["wallet_id"].(string); ok {
				return w
			}
		}
	}
	return ""
}

func getWalletBalance(t *testing.T, phone string) int64 {
	req, _ := http.NewRequest(http.MethodGet, WalletSvcURL+"/v1/wallet/phone/"+phone, nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to get wallet: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected status getting wallet: %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	balance, _ := data["balance"].(float64)
	return int64(balance)
}

func deposit(t *testing.T, phone string, amount int64) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"phone_number": phone,
		"amount":       amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, TxSvcURL+"/v1/transactions/deposit", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to deposit: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
}

func withdraw(t *testing.T, phone string, amount int64) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"phone_number": phone,
		"amount":       amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, TxSvcURL+"/v1/transactions/withdraw", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to withdraw: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
}

func modifyWalletBalance(t *testing.T, phone string, amount int64, idempotencyKey string) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"phone_number": phone,
		"amount":       amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPatch, WalletSvcURL+"/v1/wallet/balance", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to modify balance: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
}

func TestSetup_CreateWallets(t *testing.T) {
	t.Log("Setting up 3 wallets...")
	createWallet(t, "01012345678", "User One", "29901011234567")
	createWallet(t, "01112345678", "User Two", "29902021234567")
	createWallet(t, "01212345678", "User Three", "29903031234567")
	createWallet(t, "01512345678", "User Four", "29904041234567")
	t.Log("Wallets created successfully.")
}

func TestAtomicity_DepositCreatesTransactionAndUpdatesWallet(t *testing.T) {
	phone := "01012345678"
	t.Logf("Testing atomicity for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)

	status, data := deposit(t, phone, 1000)
	if status != http.StatusCreated {
		t.Fatalf("Expected 201 Created, got %d. Response: %v", status, data)
	}

	txStatus, _ := data["status"].(string)
	if txStatus != "POSTED" {
		t.Errorf("Expected transaction status POSTED, got %s", txStatus)
	}

	finalBal := getWalletBalance(t, phone)
	if finalBal != initialBal+1000 {
		t.Errorf("Expected balance %d, got %d", initialBal+1000, finalBal)
	}
	t.Logf("Final balance: %d", finalBal)
}

func TestConsistency_SequentialDepositsAndWithdrawals(t *testing.T) {
	phone := "01012345678"
	t.Logf("Testing sequential consistency for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)

	deposit(t, phone, 5000)
	b1 := getWalletBalance(t, phone)
	if b1 != initialBal+5000 {
		t.Errorf("Expected %d after 5000 deposit, got %d", initialBal+5000, b1)
	}

	withdraw(t, phone, 2000)
	b2 := getWalletBalance(t, phone)
	if b2 != b1-2000 {
		t.Errorf("Expected %d after 2000 withdraw, got %d", b1-2000, b2)
	}

	deposit(t, phone, 1000)
	b3 := getWalletBalance(t, phone)
	if b3 != b2+1000 {
		t.Errorf("Expected %d after 1000 deposit, got %d", b2+1000, b3)
	}

	withdraw(t, phone, 500)
	finalBal := getWalletBalance(t, phone)
	if finalBal != b3-500 {
		t.Errorf("Expected %d after 500 withdraw, got %d", b3-500, finalBal)
	}

	expectedFinal := initialBal + 5000 - 2000 + 1000 - 500
	if finalBal != expectedFinal {
		t.Errorf("Expected final balance %d, got %d", expectedFinal, finalBal)
	}
	t.Logf("Final balance: %d", finalBal)
}

func TestIsolation_ConcurrentDepositsOnSameWallet(t *testing.T) {
	phone := "01012345678"
	t.Logf("Testing 20 concurrent deposits for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := deposit(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)
	expectedBal := initialBal + int64(successes*100)

	t.Logf("Concurrent deposits done. Successes: %d, Failures: %d", successes, failures)
	t.Logf("Final balance: %d. Expected balance: %d", finalBal, expectedBal)

	if finalBal != expectedBal {
		t.Errorf("Balance mismatch! Expected %d, got %d", expectedBal, finalBal)
	}
	if int(successes) != concurrency {
		t.Logf("Note: Not all deposits succeeded, %d retries exhausted/failed", failures)
	}
}

func TestIsolation_ConcurrentWithdrawalsOnSameWallet(t *testing.T) {
	phone := "01112345678"
	t.Logf("Testing 20 concurrent withdrawals for wallet %s", phone)

	// Ensure enough balance
	deposit(t, phone, 10000)
	initialBal := getWalletBalance(t, phone)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := withdraw(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)
	expectedBal := initialBal - int64(successes*100)

	t.Logf("Concurrent withdrawals done. Successes: %d, Failures: %d", successes, failures)
	t.Logf("Final balance: %d. Expected balance: %d", finalBal, expectedBal)

	if finalBal != expectedBal {
		t.Errorf("Balance mismatch! Expected %d, got %d", expectedBal, finalBal)
	}
}

func TestIsolation_ConcurrentMixedOperationsOnSameWallet(t *testing.T) {
	phone := "01212345678"
	t.Logf("Testing concurrent mixed operations (20 deposits, 20 withdrawals) for wallet %s", phone)

	deposit(t, phone, 50000)
	initialBal := getWalletBalance(t, phone)

	var wg sync.WaitGroup
	var dSucc, dFail, wSucc, wFail int32
	dConc := 20
	wConc := 20

	for i := 0; i < dConc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := deposit(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&dSucc, 1)
			} else {
				atomic.AddInt32(&dFail, 1)
			}
		}()
	}
	for i := 0; i < wConc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := withdraw(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&wSucc, 1)
			} else {
				atomic.AddInt32(&wFail, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)
	expectedBal := initialBal + int64(dSucc*100) - int64(wSucc*100)

	t.Logf("Mixed done. Dep Succ: %d, Fail: %d. W/D Succ: %d, Fail: %d", dSucc, dFail, wSucc, wFail)
	t.Logf("Final balance: %d. Expected balance: %d", finalBal, expectedBal)

	if finalBal != expectedBal {
		t.Errorf("Balance mismatch! Expected %d, got %d", expectedBal, finalBal)
	}
}

func TestDurability_IdempotencyOnWalletService(t *testing.T) {
	phone := "01012345678"
	t.Logf("Testing idempotency directly on wallet %s", phone)

	initialBal := getWalletBalance(t, phone)
	idempotencyKey := fmt.Sprintf("test-idem-%d", time.Now().UnixNano())

	status1, _ := modifyWalletBalance(t, phone, 150, idempotencyKey)
	if status1 != http.StatusOK {
		t.Fatalf("First modify failed with status %d", status1)
	}

	b1 := getWalletBalance(t, phone)
	if b1 != initialBal+150 {
		t.Errorf("Balance did not update on first modify. Exp %d, got %d", initialBal+150, b1)
	}

	// Send same request again
	status2, _ := modifyWalletBalance(t, phone, 150, idempotencyKey)
	if status2 != http.StatusOK {
		t.Logf("Second modify returned %d (might be valid if it rejects duplicates differently, but usually should be 200 idempotent)", status2)
	}

	finalBal := getWalletBalance(t, phone)
	if finalBal != b1 {
		t.Errorf("Idempotency failed! Balance changed on duplicate request. Initial+150=%d, final=%d", b1, finalBal)
	}
	t.Logf("Final balance: %d (Idempotency intact)", finalBal)
}

func TestEdgeCase_WithdrawMoreThanBalance(t *testing.T) {
	phone := "01512345678" // New wallet, balance 0
	t.Logf("Testing withdraw > balance on wallet %s", phone)

	status, data := withdraw(t, phone, 100)
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity && status != http.StatusInternalServerError {
		t.Errorf("Expected failure status (e.g. 400), got %d. Data: %v", status, data)
	} else {
		t.Logf("Correctly got failure status %d. Data: %v", status, data)
	}
}

func TestEdgeCase_DepositExceedsMax(t *testing.T) {
	phone := "01512345678"
	t.Logf("Testing deposit exceeding max capacity on wallet %s", phone)

	// Deposit large amount
	deposit(t, phone, 9000000000000000)

	// Try to exceed
	status, data := deposit(t, phone, 1000)
	if status == http.StatusCreated {
		t.Errorf("Expected failure when exceeding capacity, but it succeeded")
	} else {
		t.Logf("Correctly prevented max capacity overflow. Status: %d, Data: %v", status, data)
	}
}

func TestHighConcurrency_50DepositsOnSameWallet(t *testing.T) {
	phone := "01012345678"
	t.Logf("Stress testing 50 concurrent deposits for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 50

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := deposit(t, phone, 10)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)
	expectedBal := initialBal + int64(successes*10)

	t.Logf("Stress test done. Successes: %d, Failures: %d", successes, failures)
	t.Logf("Final balance: %d. Expected balance: %d", finalBal, expectedBal)

	if finalBal != expectedBal {
		t.Errorf("Balance mismatch! Expected %d, got %d", expectedBal, finalBal)
	}
}
