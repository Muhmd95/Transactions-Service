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
var reqCounter int64

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
	if err := json.Unmarshal(respBody, &data); err != nil {
		t.Fatalf("Failed to unmarshal wallet response: %v", err)
	}

	balanceVal, ok := data["balance"]
	if !ok {
		t.Fatalf("balance field missing from response: %s", string(respBody))
	}

	balanceFloat, ok := balanceVal.(float64)
	if !ok {
		t.Fatalf("balance is not a number. Got type %T, value: %v", balanceVal, balanceVal)
	}

	return int64(balanceFloat)
}

func deposit(t *testing.T, phone string, amount int64, keys ...string) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"phone_number": phone,
		"amount":       amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, TxSvcURL+"/v1/transactions/deposit", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	idemKey := fmt.Sprintf("test-idem-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))
	if len(keys) > 0 {
		idemKey = keys[0]
	}
	req.Header.Set("Idempotency-Key", idemKey)

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

func withdraw(t *testing.T, phone string, amount int64, keys ...string) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"phone_number": phone,
		"amount":       amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, TxSvcURL+"/v1/transactions/withdraw", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	idemKey := fmt.Sprintf("test-idem-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))
	if len(keys) > 0 {
		idemKey = keys[0]
	}
	req.Header.Set("Idempotency-Key", idemKey)

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

func TestDurability_IdempotencyOnTransactionService(t *testing.T) {
	phone := "01012345678"
	t.Logf("Testing idempotency directly on Transactions Service for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)
	idempotencyKey := fmt.Sprintf("tx-idem-%d", time.Now().UnixNano())

	// First request
	status1, _ := deposit(t, phone, 300, idempotencyKey)
	if status1 != http.StatusCreated && status1 != http.StatusOK {
		t.Fatalf("First deposit failed with status %d", status1)
	}

	b1 := getWalletBalance(t, phone)
	if b1 != initialBal+300 {
		t.Errorf("Balance did not update on first deposit. Exp %d, got %d", initialBal+300, b1)
	}

	// Send exactly same request again with SAME idempotency key
	status2, _ := deposit(t, phone, 300, idempotencyKey)
	if status2 != http.StatusCreated && status2 != http.StatusOK {
		t.Logf("Second deposit returned unexpected status %d", status2)
	}

	finalBal := getWalletBalance(t, phone)
	if finalBal != b1 {
		t.Errorf("Transactions Idempotency failed! Balance changed on duplicate request. Expected %d, got %d", b1, finalBal)
	}
	t.Logf("Final balance: %d (Transactions Idempotency intact)", finalBal)
}

func TestIsolation_ConcurrentIdempotentRequests(t *testing.T) {
	phone := "01012345678"
	t.Logf("Stress testing 20 concurrent identical idempotency requests for wallet %s", phone)

	initialBal := getWalletBalance(t, phone)
	idempotencyKey := fmt.Sprintf("tx-idem-concurrent-%d", time.Now().UnixNano())

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := deposit(t, phone, 100, idempotencyKey)
			if status == http.StatusCreated || status == http.StatusOK {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)
	expectedBal := initialBal + 100 // Only ONE of the 20 should have actually processed the math

	t.Logf("Concurrent idempotency test done. Successes (200/201): %d, Failures: %d", successes, failures)
	t.Logf("Final balance: %d. Expected balance: %d", finalBal, expectedBal)

	if finalBal != expectedBal {
		t.Errorf("Idempotency failure! Balance mismatch! Expected %d, got %d", expectedBal, finalBal)
	}
}

func TestEdgeCase_WithdrawMoreThanBalance(t *testing.T) {
	phone := "01512345678" // Wallet might have balance from previous runs
	t.Logf("Testing withdraw > balance on wallet %s", phone)

	currentBal := getWalletBalance(t, phone)

	status, data := withdraw(t, phone, currentBal+100)
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

func transfer(t *testing.T, senderPhone, receiverPhone string, amount int64, keys ...string) (int, map[string]interface{}) {
	payload := map[string]interface{}{
		"sender_phone":   senderPhone,
		"receiver_phone": receiverPhone,
		"amount":         amount,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, TxSvcURL+"/v1/transactions/transfer", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	idemKey := fmt.Sprintf("test-idem-transfer-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))
	if len(keys) > 0 {
		idemKey = keys[0]
	}
	req.Header.Set("Idempotency-Key", idemKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to transfer: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
}

// ============================================================================
// Transfer Tests
// ============================================================================

func TestSetup_CreateTransferWallets(t *testing.T) {
	t.Log("Setting up wallets for Transfer tests...")
	createWallet(t, "01055555555", "Transfer Sender", "29905051234567")
	createWallet(t, "01166666666", "Transfer Receiver", "29906061234567")

	// Pre-fund the sender with enough balance for all transfer tests
	status, _ := deposit(t, "01055555555", 50000)
	if status != http.StatusCreated {
		t.Fatalf("Failed to pre-fund sender. Got status: %d", status)
	}
	t.Log("Transfer wallets created and sender funded with 50000.")
}

func TestAtomicity_Transfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"

	initialSenderBal := getWalletBalance(t, senderPhone)
	initialReceiverBal := getWalletBalance(t, receiverPhone)

	status, data := transfer(t, senderPhone, receiverPhone, 500)
	if status != http.StatusCreated {
		t.Fatalf("Expected 201 Created for transfer, got %d. Response: %v", status, data)
	}

	finalSenderBal := getWalletBalance(t, senderPhone)
	finalReceiverBal := getWalletBalance(t, receiverPhone)

	if finalSenderBal != initialSenderBal-500 {
		t.Errorf("Sender balance mismatch: Expected %d, got %d", initialSenderBal-500, finalSenderBal)
	}
	if finalReceiverBal != initialReceiverBal+500 {
		t.Errorf("Receiver balance mismatch: Expected %d, got %d", initialReceiverBal+500, finalReceiverBal)
	}
	t.Logf("Sender: %d -> %d | Receiver: %d -> %d", initialSenderBal, finalSenderBal, initialReceiverBal, finalReceiverBal)
}

func TestConsistency_TransferInsufficientBalance(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"

	initialSenderBal := getWalletBalance(t, senderPhone)
	initialReceiverBal := getWalletBalance(t, receiverPhone)

	// Try to transfer more than the sender has
	status, data := transfer(t, senderPhone, receiverPhone, initialSenderBal+1000)
	if status == http.StatusCreated {
		t.Fatalf("Transfer should have failed for insufficient balance, but got 201. Response: %v", data)
	}
	t.Logf("Correctly rejected with status %d: %v", status, data)

	// Verify neither balance changed
	finalSenderBal := getWalletBalance(t, senderPhone)
	finalReceiverBal := getWalletBalance(t, receiverPhone)

	if finalSenderBal != initialSenderBal {
		t.Errorf("Sender balance should not change on failed transfer! Expected %d, got %d", initialSenderBal, finalSenderBal)
	}
	if finalReceiverBal != initialReceiverBal {
		t.Errorf("Receiver balance should not change on failed transfer! Expected %d, got %d", initialReceiverBal, finalReceiverBal)
	}
}

func TestConsistency_TransferMaxBalanceFails(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"

	// Top up receiver to max capacity
	currentReceiverBal := getWalletBalance(t, receiverPhone)
	amountToMax := int64(9000000000000000) - currentReceiverBal
	if amountToMax > 0 {
		status, _ := deposit(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to top up receiver to max capacity! Got status: %d", status)
		}
	}

	initialSenderBal := getWalletBalance(t, senderPhone)

	// Attempt transfer that would push receiver over max
	status, data := transfer(t, senderPhone, receiverPhone, 50)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("Expected 422, got %d. Response: %v", status, data)
	}
	t.Logf("Correctly rejected with status %d", status)

	// Verify sender balance was NOT affected (atomic rollback)
	finalSenderBal := getWalletBalance(t, senderPhone)
	if finalSenderBal != initialSenderBal {
		t.Errorf("Sender balance should not change on failed transfer! Expected %d, got %d", initialSenderBal, finalSenderBal)
	}

	// Reset receiver back down so future tests work
	withdraw(t, receiverPhone, amountToMax)
}

func TestIsolation_ConcurrentTransfers(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"

	// Top up sender with extra funds for concurrent transfers
	status, _ := deposit(t, senderPhone, 5000)
	if status != http.StatusCreated {
		t.Fatalf("Failed to top up sender for concurrent test. Got status: %d", status)
	}

	initialSenderBal := getWalletBalance(t, senderPhone)
	initialReceiverBal := getWalletBalance(t, receiverPhone)

	var wg sync.WaitGroup
	concurrency := 20
	var successes, failures int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := transfer(t, senderPhone, receiverPhone, 50)
			if s == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalSenderBal := getWalletBalance(t, senderPhone)
	finalReceiverBal := getWalletBalance(t, receiverPhone)

	expectedSenderBal := initialSenderBal - int64(successes)*50
	expectedReceiverBal := initialReceiverBal + int64(successes)*50

	t.Logf("Concurrent transfers done. Successes: %d, Failures: %d", successes, failures)
	t.Logf("Sender: %d -> %d (expected %d) | Receiver: %d -> %d (expected %d)",
		initialSenderBal, finalSenderBal, expectedSenderBal,
		initialReceiverBal, finalReceiverBal, expectedReceiverBal)

	if finalSenderBal != expectedSenderBal {
		t.Errorf("Concurrent Transfer Sender mismatch! Expected %d, got %d", expectedSenderBal, finalSenderBal)
	}
	if finalReceiverBal != expectedReceiverBal {
		t.Errorf("Concurrent Transfer Receiver mismatch! Expected %d, got %d", expectedReceiverBal, finalReceiverBal)
	}
}

func TestDurability_IdempotentTransfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	idemKey := fmt.Sprintf("test-idem-transfer-ok-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))

	initialSenderBal := getWalletBalance(t, senderPhone)
	initialReceiverBal := getWalletBalance(t, receiverPhone)

	// First request
	status1, data1 := transfer(t, senderPhone, receiverPhone, 100, idemKey)
	if status1 != http.StatusCreated {
		t.Fatalf("First transfer failed with status %d: %v", status1, data1)
	}

	// Second request with SAME idempotency key
	status2, data2 := transfer(t, senderPhone, receiverPhone, 100, idemKey)
	if status2 != http.StatusCreated {
		t.Fatalf("Idempotent retry failed with status %d: %v", status2, data2)
	}

	// Verify same transaction was returned
	if data1["transaction_id"] != data2["transaction_id"] {
		t.Errorf("Transaction IDs do not match! First: %v, Second: %v", data1["transaction_id"], data2["transaction_id"])
	}

	// Ensure balance was only changed ONCE
	finalSenderBal := getWalletBalance(t, senderPhone)
	finalReceiverBal := getWalletBalance(t, receiverPhone)

	if finalSenderBal != initialSenderBal-100 {
		t.Errorf("Sender charged multiple times! Expected %d, got %d", initialSenderBal-100, finalSenderBal)
	}
	if finalReceiverBal != initialReceiverBal+100 {
		t.Errorf("Receiver credited multiple times! Expected %d, got %d", initialReceiverBal+100, finalReceiverBal)
	}
	t.Logf("Idempotency intact. Sender: %d -> %d | Receiver: %d -> %d", initialSenderBal, finalSenderBal, initialReceiverBal, finalReceiverBal)
}

func TestDurability_IdempotentFailedTransfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	idemKey := fmt.Sprintf("test-idem-transfer-fail-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))

	// Top up receiver to max
	currentReceiverBal := getWalletBalance(t, receiverPhone)
	amountToMax := int64(9000000000000000) - currentReceiverBal
	if amountToMax > 0 {
		status, _ := deposit(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to top up receiver to max! Got status: %d", status)
		}
	}

	initialSenderBal := getWalletBalance(t, senderPhone)

	// First request — should fail with 422
	status1, data1 := transfer(t, senderPhone, receiverPhone, 50, idemKey)
	if status1 != http.StatusUnprocessableEntity {
		t.Fatalf("Expected 422 for first failed transfer, got %d. Response: %v", status1, data1)
	}

	// Second request with SAME idempotency key — should also fail with 422
	status2, data2 := transfer(t, senderPhone, receiverPhone, 50, idemKey)
	if status2 != http.StatusUnprocessableEntity {
		t.Fatalf("Expected 422 for idempotent retry, got %d. Response: %v", status2, data2)
	}

	// Error messages should match
	if data1["error"] != data2["error"] {
		t.Errorf("Idempotent error messages don't match: %v vs %v", data1["error"], data2["error"])
	}

	// Sender balance should be unchanged (the FAILED tx has BalanceBefore == BalanceAfter)
	finalSenderBal := getWalletBalance(t, senderPhone)
	if finalSenderBal != initialSenderBal {
		t.Errorf("Sender balance should not change on failed transfer! Expected %d, got %d", initialSenderBal, finalSenderBal)
	}

	t.Logf("Idempotent failure intact. Status: %d, Error: %v", status2, data2["error"])

	// Reset receiver back down
	withdraw(t, receiverPhone, amountToMax)
}

func TestEdgeCase_TransferToSelf(t *testing.T) {
	phone := "01055555555"

	initialBal := getWalletBalance(t, phone)

	status, data := transfer(t, phone, phone, 100)
	if status != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request for self-transfer, got %d. Response: %v", status, data)
	}
	t.Logf("Correctly rejected self-transfer with status 400 Bad Request: %v", data)

	// Balance should be unchanged
	finalBal := getWalletBalance(t, phone)
	if finalBal != initialBal {
		t.Errorf("Balance should not change on self-transfer! Expected %d, got %d", initialBal, finalBal)
	}
}

func TestIsolation_ConcurrentWithdrawalsOnlyOneSucceeds(t *testing.T) {
	phone := "01027272727"
	depositAmount := int64(5000)
	t.Logf("Testing 20 concurrent full-balance withdrawals on wallet %s (only 1 should succeed)", phone)

	// Create a fresh wallet for this test
	createWallet(t, phone, "Race Withdraw User", "29907071234567")

	// Deposit a known amount so the wallet has exactly depositAmount available
	status, _ := deposit(t, phone, depositAmount)
	if status != http.StatusCreated {
		t.Fatalf("Failed to pre-fund wallet. Got status: %d", status)
	}

	initialBal := getWalletBalance(t, phone)
	t.Logf("Initial balance after deposit: %d", initialBal)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20

	// All 20 goroutines try to withdraw the entire balance at once
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := withdraw(t, phone, initialBal)
			if s == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	finalBal := getWalletBalance(t, phone)

	t.Logf("Concurrent withdrawals done. Successes: %d, Failures: %d", successes, failures)
	t.Logf("Final balance: %d", finalBal)

	// Exactly 1 should succeed
	if successes != 1 {
		t.Errorf("Expected exactly 1 successful withdrawal, got %d", successes)
	}

	// The remaining 19 should have failed
	if failures != int32(concurrency-1) {
		t.Errorf("Expected %d failures, got %d", concurrency-1, failures)
	}

	// Balance should be 0
	if finalBal != 0 {
		t.Errorf("Expected final balance 0, got %d", finalBal)
	}
}


