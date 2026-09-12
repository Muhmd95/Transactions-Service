//go:build integration

// Requires the live stack: Wallet Service (:8000), Transactions Service
// (:8080), and the Kafka CDC pipeline. Run with:
//
//	go test -tags=integration -count=1 ./tests/
//
// Assertion strategy (matches the CDC architecture):
//   - Ledger-first: balance assertions read the "balance" / "balance_after"
//     field of the Transactions Service response, which is committed
//     synchronously with the ledger write.
//   - Convergence: each test ends with waitForBalance, polling the Wallet
//     Service until the CDC pipeline (Mongo change stream -> Kafka Connect
//     -> wallet consumer) has propagated the ledger state. This also
//     guarantees the next test starts from a fresh (non-stale) wallet read.
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	WalletSvcURL = "http://localhost:8000"
	TxSvcURL     = "http://localhost:8080"

	// mirrors the transactions service WalletMax
	walletMaxBalance = int64(9000000000000000)

	// wallet balance is an eventually-consistent CDC projection of the ledger
	convergeTimeout = 15 * time.Second
	convergePoll    = 250 * time.Millisecond
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

// getWalletBalanceSoft is a non-fatal read used by waitForBalance and the
// balanceBefore helpers.
func getWalletBalanceSoft(phone string) (int64, bool) {
	req, err := http.NewRequest(http.MethodGet, WalletSvcURL+"/v1/wallet/phone/"+phone, nil)
	if err != nil {
		return 0, false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false
	}
	var data map[string]interface{}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return 0, false
	}
	balanceFloat, ok := data["balance"].(float64)
	if !ok {
		return 0, false
	}
	return int64(balanceFloat), true
}

// logBalanceBefore prints the starting balance of each wallet under test,
// read from the Wallet Service immediately before the observation window
// opens — the suite guarantees the wallet projection has converged by then,
// so the value equals the ledger state at test start. Prints "?" when the
// wallet does not exist yet (clean database, first run).
func logBalanceBefore(t *testing.T, phones ...string) {
	t.Helper()
	for _, p := range phones {
		if bal, ok := getWalletBalanceSoft(p); ok {
			t.Logf(" balance_before : %s = %d", p, bal)
		} else {
			t.Logf(" balance_before : %s = ?", p)
		}
	}
}

// mustBalanceBefore is logBalanceBefore for a single wallet whose starting
// balance the test needs: it logs the value and returns it, failing the test
// if the wallet is not readable. Like logBalanceBefore it must be called
// BEFORE beginObs so the read stays outside the observation.
func mustBalanceBefore(t *testing.T, phone string) int64 {
	t.Helper()
	bal, ok := getWalletBalanceSoft(phone)
	if !ok {
		t.Fatalf("wallet %s not readable before test", phone)
	}
	t.Logf(" balance_before : %s = %d", phone, bal)
	return bal
}

// waitForBalance polls until the wallet (CDC projection of the ledger)
// converges to the expected balance, or fails the test on timeout.
func waitForBalance(t *testing.T, phone string, want int64) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(convergeTimeout)
	lastSeen, seen := int64(0), false
	for time.Now().Before(deadline) {
		if bal, ok := getWalletBalanceSoft(phone); ok {
			lastSeen, seen = bal, true
			if bal == want {
				recordConverge(phone, want, time.Since(started), true, lastSeen)
				return
			}
		}
		time.Sleep(convergePoll)
	}
	recordConverge(phone, want, time.Since(started), false, lastSeen)
	t.Errorf("wallet %s did not converge to %d (last seen %d, seen=%v) within %s — CDC lag or dropped event",
		phone, want, lastSeen, seen, convergeTimeout)
}

// responseInt64 extracts a numeric field (e.g. "balance", "balance_after")
// from a JSON response body.
func responseInt64(data map[string]interface{}, field string) (int64, bool) {
	if data == nil {
		return 0, false
	}
	v, ok := data[field].(float64)
	if !ok {
		return 0, false
	}
	return int64(v), true
}

// deposit/withdraw/transfer use t.Errorf (never t.Fatalf) so they are safe
// to call from goroutines in the concurrency tests.

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

	sent := time.Now()
	resp, err := httpClient.Do(req)
	done := time.Now()
	if err != nil {
		recordReq(0, sent, done)
		t.Errorf("Failed to deposit: %v", err)
		return 0, nil
	}
	recordReq(resp.StatusCode, sent, done)
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

	sent := time.Now()
	resp, err := httpClient.Do(req)
	done := time.Now()
	if err != nil {
		recordReq(0, sent, done)
		t.Errorf("Failed to withdraw: %v", err)
		return 0, nil
	}
	recordReq(resp.StatusCode, sent, done)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
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

	sent := time.Now()
	resp, err := httpClient.Do(req)
	done := time.Now()
	if err != nil {
		recordReq(0, sent, done)
		t.Errorf("Failed to transfer: %v", err)
		return 0, nil
	}
	recordReq(resp.StatusCode, sent, done)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	json.Unmarshal(respBody, &data)

	return resp.StatusCode, data
}

func TestSetup_CreateWallets(t *testing.T) {
	o := beginObs(t, "type: SETUP | creates 4 wallets: 01012345678, 01112345678, 01212345678, 01512345678 (no-op if they exist)")
	defer o.report(t)
	createWallet(t, "01012345678", "User One", "29901011234567")
	createWallet(t, "01112345678", "User Two", "29902021234567")
	createWallet(t, "01212345678", "User Three", "29903031234567")
	createWallet(t, "01512345678", "User Four", "29904041234567")
}

func TestAtomicity_DepositCreatesTransactionAndUpdatesWallet(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 1x DEPOSIT 1000 | expect: 201 POSTED, balance+1000", phone))
	defer o.report(t)

	status, data := deposit(t, phone, 1000)
	if status != http.StatusCreated {
		t.Fatalf("Expected 201 Created, got %d. Response: %v", status, data)
	}

	txStatus, _ := data["status"].(string)
	if txStatus != "POSTED" {
		t.Errorf("Expected transaction status POSTED, got %s", txStatus)
	}

	respBal, ok := responseInt64(data, "balance")
	if !ok || respBal != initialBal+1000 {
		t.Errorf("Expected ledger balance %d, got %v", initialBal+1000, data["balance"])
	}

	waitForBalance(t, phone, initialBal+1000)
}

func TestConsistency_SequentialDepositsAndWithdrawals(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: DEPOSIT 5000, WITHDRAW 2000, DEPOSIT 1000, WITHDRAW 500 (sequential) | expect: net +3500", phone))
	defer o.report(t)

	s1, d1 := deposit(t, phone, 5000)
	if s1 != http.StatusCreated {
		t.Fatalf("Deposit failed with status %d: %v", s1, d1)
	}
	b1, ok := responseInt64(d1, "balance")
	if !ok || b1 != initialBal+5000 {
		t.Errorf("Expected %d after 5000 deposit, got %v", initialBal+5000, d1["balance"])
	}

	s2, d2 := withdraw(t, phone, 2000)
	if s2 != http.StatusCreated {
		t.Fatalf("Withdrawal failed with status %d: %v", s2, d2)
	}
	b2, ok := responseInt64(d2, "balance")
	if !ok || b2 != b1-2000 {
		t.Errorf("Expected %d after 2000 withdraw, got %v", b1-2000, d2["balance"])
	}

	s3, d3 := deposit(t, phone, 1000)
	if s3 != http.StatusCreated {
		t.Fatalf("Deposit failed with status %d: %v", s3, d3)
	}
	b3, ok := responseInt64(d3, "balance")
	if !ok || b3 != b2+1000 {
		t.Errorf("Expected %d after 1000 deposit, got %v", b2+1000, d3["balance"])
	}

	s4, d4 := withdraw(t, phone, 500)
	if s4 != http.StatusCreated {
		t.Fatalf("Withdrawal failed with status %d: %v", s4, d4)
	}
	b4, ok := responseInt64(d4, "balance")
	if !ok || b4 != b3-500 {
		t.Errorf("Expected %d after 500 withdraw, got %v", b3-500, d4["balance"])
	}

	expectedFinal := initialBal + 5000 - 2000 + 1000 - 500
	if b4 != expectedFinal {
		t.Errorf("Expected final ledger balance %d, got %d", expectedFinal, b4)
	}

	waitForBalance(t, phone, expectedFinal)
}

func TestIsolation_ConcurrentDepositsOnSameWallet(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 20x concurrent DEPOSIT 100 | expect: baseline+successes*100", phone))
	defer o.report(t)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20
	var mu sync.Mutex
	ledgerBalances := make([]int64, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, data := deposit(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
				if b, ok := responseInt64(data, "balance"); ok {
					mu.Lock()
					ledgerBalances = append(ledgerBalances, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	expectedBal := initialBal + int64(successes)*100

	if len(ledgerBalances) > 0 && slices.Max(ledgerBalances) != expectedBal {
		t.Errorf("Ledger mismatch! Max ledger balance %d != expected %d", slices.Max(ledgerBalances), expectedBal)
	}

	waitForBalance(t, phone, expectedBal)
}

func TestIsolation_50ConcurrentWithdrawalsOnSameWallet(t *testing.T) {
	phone := "01112345678"
	logBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | setup: 1x DEPOSIT 10000 | ops: 50x concurrent WITHDRAW 100 | expect: baseline-successes*100", phone))
	defer o.report(t)

	// Fund the wallet and take the ledger baseline from the response
	// (synchronous), not from a wallet read (eventually consistent).
	s0, d0 := deposit(t, phone, 10000)
	if s0 != http.StatusCreated {
		t.Fatalf("Failed to fund wallet for withdrawal test, status %d: %v", s0, d0)
	}
	baselineBal, ok := responseInt64(d0, "balance")
	if !ok {
		t.Fatalf("Funding deposit response missing balance: %v", d0)
	}

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 50
	var mu sync.Mutex
	ledgerBalances := make([]int64, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, data := withdraw(t, phone, 100)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
				if b, ok := responseInt64(data, "balance"); ok {
					mu.Lock()
					ledgerBalances = append(ledgerBalances, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	expectedBal := baselineBal - int64(successes)*100

	if len(ledgerBalances) > 0 && slices.Min(ledgerBalances) != expectedBal {
		t.Errorf("Ledger mismatch! Min ledger balance %d != expected %d", slices.Min(ledgerBalances), expectedBal)
	}

	waitForBalance(t, phone, expectedBal)
}

func TestIsolation_ConcurrentMixedOperationsOnSameWallet(t *testing.T) {
	phone := "01212345678"
	logBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | setup: 1x DEPOSIT 50000 | ops: 20x concurrent DEPOSIT 100 + 20x concurrent WITHDRAW 100 | expect: baseline+net", phone))
	defer o.report(t)

	s0, d0 := deposit(t, phone, 50000)
	if s0 != http.StatusCreated {
		t.Fatalf("Failed to fund wallet for mixed test, status %d: %v", s0, d0)
	}
	baselineBal, ok := responseInt64(d0, "balance")
	if !ok {
		t.Fatalf("Funding deposit response missing balance: %v", d0)
	}

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

	expectedBal := baselineBal + int64(dSucc)*100 - int64(wSucc)*100

	waitForBalance(t, phone, expectedBal)
}

func TestDurability_IdempotencyOnTransactionService(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 2x DEPOSIT 300, same idem-key | expect: 1 POST, same tx_id, balance+300", phone))
	defer o.report(t)

	idempotencyKey := fmt.Sprintf("tx-idem-%d", time.Now().UnixNano())

	// First request
	status1, data1 := deposit(t, phone, 300, idempotencyKey)
	if status1 != http.StatusCreated && status1 != http.StatusOK {
		t.Fatalf("First deposit failed with status %d: %v", status1, data1)
	}
	b1, ok := responseInt64(data1, "balance")
	if !ok || b1 != initialBal+300 {
		t.Errorf("Balance mismatch on first deposit. Exp %d, got %v", initialBal+300, data1["balance"])
	}

	// Send exactly same request again with SAME idempotency key
	status2, data2 := deposit(t, phone, 300, idempotencyKey)
	if status2 != http.StatusCreated && status2 != http.StatusOK {
		t.Logf("Second deposit returned unexpected status %d: %v", status2, data2)
	}
	if status2 == http.StatusCreated || status2 == http.StatusOK {
		if data1["transaction_id"] != data2["transaction_id"] {
			t.Errorf("Idempotent retry returned different transaction IDs: %v vs %v",
				data1["transaction_id"], data2["transaction_id"])
		}
		b2, ok := responseInt64(data2, "balance")
		if !ok || b2 != b1 {
			t.Errorf("Transactions Idempotency failed! Balance changed on duplicate request. First %d, retry %v", b1, data2["balance"])
		}
	}

	waitForBalance(t, phone, initialBal+300)
}

func TestIsolation_ConcurrentIdempotentRequests(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 20x concurrent DEPOSIT 100, same idem-key | expect: 1 POST only, balance+100", phone))
	defer o.report(t)

	idempotencyKey := fmt.Sprintf("tx-idem-concurrent-%d", time.Now().UnixNano())

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20
	var mu sync.Mutex
	txIDs := make([]string, 0, concurrency)
	ledgerBalances := make([]int64, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, data := deposit(t, phone, 100, idempotencyKey)
			if status == http.StatusCreated || status == http.StatusOK {
				atomic.AddInt32(&successes, 1)
				if id, ok := data["transaction_id"].(string); ok {
					mu.Lock()
					txIDs = append(txIDs, id)
					mu.Unlock()
				}
				if b, ok := responseInt64(data, "balance"); ok {
					mu.Lock()
					ledgerBalances = append(ledgerBalances, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	// Only ONE of the 20 should have actually processed the math
	expectedBal := initialBal + 100

	if len(txIDs) > 1 {
		for _, id := range txIDs[1:] {
			if id != txIDs[0] {
				t.Errorf("Concurrent idempotent retries returned different transaction IDs: %s vs %s", txIDs[0], id)
				break
			}
		}
	}
	for _, b := range ledgerBalances {
		if b != expectedBal {
			t.Errorf("Idempotency failure! Ledger balance %d != expected %d", b, expectedBal)
			break
		}
	}

	waitForBalance(t, phone, expectedBal)
}

func TestEdgeCase_WithdrawMoreThanBalance(t *testing.T) {
	phone := "01512345678" // Wallet might have balance from previous runs
	currentBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 1x WITHDRAW balance+100 | expect: 4xx reject, balance unchanged", phone))
	defer o.report(t)

	status, data := withdraw(t, phone, currentBal+100)
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity && status != http.StatusInternalServerError {
		t.Errorf("Expected failure status (e.g. 400), got %d. Data: %v", status, data)
	} else {
		t.Logf("Correctly got failure status %d. Data: %v", status, data)
	}

	// Ledger untouched -> wallet must still show the same balance
	waitForBalance(t, phone, currentBal)
}

func TestEdgeCase_DepositExceedsMax(t *testing.T) {
	phone := "01512345678"
	logBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: DEPOSIT up to WalletMax, then DEPOSIT 1000 | expect: second rejected", phone))
	defer o.report(t)

	// Deposit large amount
	status, data := deposit(t, phone, walletMaxBalance)
	if status == http.StatusCreated {
		bal, ok := responseInt64(data, "balance")
		if !ok || bal != walletMaxBalance {
			t.Errorf("Expected balance %d after max deposit, got %v", walletMaxBalance, data["balance"])
		}
		waitForBalance(t, phone, walletMaxBalance)
	} else {
		t.Logf("Top-up to max rejected with status %d (wallet already at max from a previous run)", status)
	}

	// Try to exceed
	status2, data2 := deposit(t, phone, 1000)
	if status2 == http.StatusCreated {
		t.Errorf("Expected failure when exceeding capacity, but it succeeded")
	} else {
		t.Logf("Correctly prevented max capacity overflow. Status: %d, Data: %v", status2, data2)
	}
}

func TestHighConcurrency_50DepositsOnSameWallet(t *testing.T) {
	phone := "01012345678"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 50x concurrent DEPOSIT 10 | expect: baseline+successes*10", phone))
	defer o.report(t)

	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 50
	var mu sync.Mutex
	ledgerBalances := make([]int64, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, data := deposit(t, phone, 10)
			if status == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
				if b, ok := responseInt64(data, "balance"); ok {
					mu.Lock()
					ledgerBalances = append(ledgerBalances, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	expectedBal := initialBal + int64(successes)*10

	if len(ledgerBalances) > 0 && slices.Max(ledgerBalances) != expectedBal {
		t.Errorf("Ledger mismatch! Max ledger balance %d != expected %d", slices.Max(ledgerBalances), expectedBal)
	}

	waitForBalance(t, phone, expectedBal)
}

// ============================================================================
// Transfer Tests
// ============================================================================

func TestSetup_CreateTransferWallets(t *testing.T) {
	createWallet(t, "01055555555", "Transfer Sender", "29905051234567")
	createWallet(t, "01166666666", "Transfer Receiver", "29906061234567")
	logBalanceBefore(t, "01055555555", "01166666666")
	o := beginObs(t, "type: SETUP | wallets: 01055555555 (sender), 01166666666 (receiver) | setup: 1x DEPOSIT 50000 to sender")
	defer o.report(t)
	
	// Pre-fund the sender with enough balance for all transfer tests
	status, data := deposit(t, "01055555555", 50000)
	if status != http.StatusCreated {
		t.Fatalf("Failed to pre-fund sender. Got status: %d, data: %v", status, data)
	}
	fundedBal, ok := responseInt64(data, "balance")
	if !ok {
		t.Fatalf("Pre-fund deposit response missing balance: %v", data)
	}

	// Converge before the next test reads the sender balance
	waitForBalance(t, "01055555555", fundedBal)
}

func TestAtomicity_Transfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	initialSenderBal := mustBalanceBefore(t, senderPhone)
	initialReceiverBal := mustBalanceBefore(t, receiverPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | ops: 1x TRANSFER 500 | expect: sender -500, receiver +500", senderPhone, receiverPhone))
	defer o.report(t)

	status, data := transfer(t, senderPhone, receiverPhone, 500)
	if status != http.StatusCreated {
		t.Fatalf("Expected 201 Created for transfer, got %d. Response: %v", status, data)
	}

	senderAfter, ok := responseInt64(data, "balance_after")
	if !ok || senderAfter != initialSenderBal-500 {
		t.Errorf("Sender ledger balance mismatch: expected %d, got %v", initialSenderBal-500, data["balance_after"])
	}

	waitForBalance(t, senderPhone, initialSenderBal-500)
	waitForBalance(t, receiverPhone, initialReceiverBal+500)
}

func TestConsistency_TransferInsufficientBalance(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	initialSenderBal := mustBalanceBefore(t, senderPhone)
	initialReceiverBal := mustBalanceBefore(t, receiverPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | ops: 1x TRANSFER sender_balance+1000 | expect: rejected, both balances unchanged", senderPhone, receiverPhone))
	defer o.report(t)

	// Try to transfer more than the sender has
	status, data := transfer(t, senderPhone, receiverPhone, initialSenderBal+1000)
	if status == http.StatusCreated {
		t.Fatalf("Transfer should have failed for insufficient balance, but got 201. Response: %v", data)
	}
	t.Logf("Correctly rejected with status %d: %v", status, data)

	// Neither balance may change
	waitForBalance(t, senderPhone, initialSenderBal)
	waitForBalance(t, receiverPhone, initialReceiverBal)
}

func TestConsistency_TransferMaxBalanceFails(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	currentReceiverBal := mustBalanceBefore(t, receiverPhone)
	initialSenderBal := mustBalanceBefore(t, senderPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | ops: top-up receiver to WalletMax, 1x TRANSFER 50, withdraw reset | expect: 422, sender unchanged", senderPhone, receiverPhone))
	defer o.report(t)

	// Top up receiver to max capacity
	amountToMax := walletMaxBalance - currentReceiverBal
	if amountToMax > 0 {
		status, data := deposit(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to top up receiver to max capacity! Got status: %d, data: %v", status, data)
		}
		if bal, ok := responseInt64(data, "balance"); !ok || bal != walletMaxBalance {
			t.Errorf("Receiver ledger balance after top-up: expected %d, got %v", walletMaxBalance, data["balance"])
		}
	}

	// Attempt transfer that would push receiver over max
	status, data := transfer(t, senderPhone, receiverPhone, 50)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("Expected 422, got %d. Response: %v", status, data)
	}
	t.Logf("Correctly rejected with status %d", status)

	// Sender untouched (atomic rollback), receiver at max
	waitForBalance(t, senderPhone, initialSenderBal)
	waitForBalance(t, receiverPhone, walletMaxBalance)

	// Reset receiver back down so future tests work
	if amountToMax > 0 {
		status, data := withdraw(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to reset receiver balance. Got status: %d, data: %v", status, data)
		}
		if bal, ok := responseInt64(data, "balance"); !ok || bal != currentReceiverBal {
			t.Errorf("Receiver reset mismatch: expected %d, got %v", currentReceiverBal, data["balance"])
		}
		waitForBalance(t, receiverPhone, currentReceiverBal)
	}
}

func TestIsolation_ConcurrentTransfers(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	logBalanceBefore(t, senderPhone)
	initialReceiverBal := mustBalanceBefore(t, receiverPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | setup: 1x DEPOSIT 5000 | ops: 20x concurrent TRANSFER 50 | expect: sender -successes*50, receiver +successes*50", senderPhone, receiverPhone))
	defer o.report(t)

	// Top up sender with extra funds for concurrent transfers
	status, data := deposit(t, senderPhone, 5000)
	if status != http.StatusCreated {
		t.Fatalf("Failed to top up sender for concurrent test. Got status: %d, data: %v", status, data)
	}
	baselineSenderBal, ok := responseInt64(data, "balance")
	if !ok {
		t.Fatalf("Top-up deposit response missing balance: %v", data)
	}

	var wg sync.WaitGroup
	concurrency := 20
	var successes, failures int32
	var mu sync.Mutex
	senderAfters := make([]int64, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, d := transfer(t, senderPhone, receiverPhone, 50)
			if s == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
				if b, ok := responseInt64(d, "balance_after"); ok {
					mu.Lock()
					senderAfters = append(senderAfters, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	expectedSenderBal := baselineSenderBal - int64(successes)*50
	expectedReceiverBal := initialReceiverBal + int64(successes)*50

	t.Logf("Concurrent transfers done. Successes: %d, Failures: %d", successes, failures)
	if len(senderAfters) > 0 && slices.Min(senderAfters) != expectedSenderBal {
		t.Errorf("Concurrent transfer sender ledger mismatch! Min balance_after %d != expected %d",
			slices.Min(senderAfters), expectedSenderBal)
	}

	waitForBalance(t, senderPhone, expectedSenderBal)
	waitForBalance(t, receiverPhone, expectedReceiverBal)
}

func TestDurability_IdempotentTransfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	idemKey := fmt.Sprintf("test-idem-transfer-ok-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))
	initialSenderBal := mustBalanceBefore(t, senderPhone)
	initialReceiverBal := mustBalanceBefore(t, receiverPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | ops: 2x TRANSFER 100, same idem-key | expect: 1 POST, same tx_id, -100/+100", senderPhone, receiverPhone))
	defer o.report(t)

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

	// The ledger balance must reflect the transfer exactly once
	after1, ok := responseInt64(data1, "balance_after")
	if !ok || after1 != initialSenderBal-100 {
		t.Errorf("Sender ledger mismatch on first transfer: expected %d, got %v", initialSenderBal-100, data1["balance_after"])
	}
	after2, ok := responseInt64(data2, "balance_after")
	if !ok || after2 != initialSenderBal-100 {
		t.Errorf("Sender ledger mismatch on idempotent retry: expected %d, got %v", initialSenderBal-100, data2["balance_after"])
	}

	waitForBalance(t, senderPhone, initialSenderBal-100)
	waitForBalance(t, receiverPhone, initialReceiverBal+100)
}

func TestDurability_IdempotentFailedTransfer(t *testing.T) {
	senderPhone := "01055555555"
	receiverPhone := "01166666666"
	idemKey := fmt.Sprintf("test-idem-transfer-fail-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&reqCounter, 1))
	currentReceiverBal := mustBalanceBefore(t, receiverPhone)
	initialSenderBal := mustBalanceBefore(t, senderPhone)
	o := beginObs(t, fmt.Sprintf("sender: %s | receiver: %s | ops: receiver to WalletMax, 2x TRANSFER 50 same idem-key, withdraw reset | expect: 422 twice, same error, balances unchanged", senderPhone, receiverPhone))
	defer o.report(t)

	// Top up receiver to max
	amountToMax := walletMaxBalance - currentReceiverBal
	if amountToMax > 0 {
		status, data := deposit(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to top up receiver to max! Got status: %d, data: %v", status, data)
		}
	}

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

	// The FAILED tx has BalanceBefore == BalanceAfter and is excluded from
	// CDC (status != POSTED), so neither wallet balance may change.
	waitForBalance(t, senderPhone, initialSenderBal)
	waitForBalance(t, receiverPhone, walletMaxBalance)

	t.Logf("Idempotent failure intact. Status: %d, Error: %v", status2, data2["error"])

	// Reset receiver back down
	if amountToMax > 0 {
		status, data := withdraw(t, receiverPhone, amountToMax)
		if status != http.StatusCreated {
			t.Fatalf("Failed to reset receiver balance. Got status: %d, data: %v", status, data)
		}
		if bal, ok := responseInt64(data, "balance"); !ok || bal != currentReceiverBal {
			t.Errorf("Receiver reset mismatch: expected %d, got %v", currentReceiverBal, data["balance"])
		}
		waitForBalance(t, receiverPhone, currentReceiverBal)
	}
}

func TestEdgeCase_TransferToSelf(t *testing.T) {
	phone := "01055555555"
	initialBal := mustBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | ops: 1x TRANSFER 100 to self | expect: 400, balance unchanged", phone))
	defer o.report(t)

	status, data := transfer(t, phone, phone, 100)
	if status != http.StatusBadRequest {
		t.Fatalf("Expected 400 Bad Request for self-transfer, got %d. Response: %v", status, data)
	}
	t.Logf("Correctly rejected self-transfer with status 400 Bad Request: %v", data)

	waitForBalance(t, phone, initialBal)
}

func TestIsolation_ConcurrentWithdrawalsOnlyOneSucceeds(t *testing.T) {
	phone := "01027272727"
	createWallet(t, phone, "Race Withdraw User", "29907071234567")
	depositAmount := int64(5000)
	logBalanceBefore(t, phone)
	o := beginObs(t, fmt.Sprintf("wallet: %s | setup: 1x DEPOSIT %d + converge | ops: 20x concurrent WITHDRAW full balance | expect: exactly 1 ok, 19 failed, wallet -> 0", phone, depositAmount))
	defer o.report(t)

	// Create a fresh wallet for this test (no-op if it already exists —
	// residual balance from a previous run is handled below).
	

	// Fund the wallet. The deposit response reports the full ledger
	// balance — including any residual balance from previous runs.
	status, data := deposit(t, phone, depositAmount)
	if status != http.StatusCreated {
		t.Fatalf("Failed to pre-fund wallet. Got status: %d, data: %v", status, data)
	}
	fundedBal, ok := responseInt64(data, "balance")
	if !ok {
		t.Fatalf("Pre-fund deposit response missing balance: %v", data)
	}
	t.Logf("Ledger balance before the race: %d", fundedBal)

	// Converge the wallet projection BEFORE racing: only a fresh (non-stale)
	// wallet read equals the ledger balance. Reading a stale wallet balance
	// and withdrawing it would let more than one withdrawal succeed.
	waitForBalance(t, phone, fundedBal)

	// waitForBalance guarantees the wallet now equals fundedBal — that is
	// the amount all 20 goroutines race to withdraw.
	var wg sync.WaitGroup
	var successes, failures int32
	concurrency := 20
	var mu sync.Mutex
	finalBalances := make([]int64, 0, 1)

	// All 20 goroutines try to withdraw the entire balance at once
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, d := withdraw(t, phone, fundedBal)
			if s == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
				if b, ok := responseInt64(d, "balance"); ok {
					mu.Lock()
					finalBalances = append(finalBalances, b)
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}
	wg.Wait()

	t.Logf("Concurrent withdrawals done. Successes: %d, Failures: %d", successes, failures)

	// Exactly 1 should succeed
	if successes != 1 {
		t.Errorf("Expected exactly 1 successful withdrawal, got %d", successes)
	}

	// The remaining 19 should have failed
	if failures != int32(concurrency-1) {
		t.Errorf("Expected %d failures, got %d", concurrency-1, failures)
	}

	// The single success must have drained the wallet to 0
	if len(finalBalances) > 0 && finalBalances[0] != 0 {
		t.Errorf("Successful withdrawal should drain balance to 0, got %d", finalBalances[0])
	}

	waitForBalance(t, phone, 0)
}
