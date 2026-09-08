# Transactions Service ACID Test Documentation

This document describes the `acid_test.go` integration tests for the Transactions Service. These tests hit the live HTTP endpoints to verify Atomicity, Consistency, Isolation, and Durability (ACID) properties of the transaction flows, especially focusing on how MongoDB handles concurrent operations.

## Architecture Context

The system is **eventually consistent by design**: the Transactions Service owns the ledger (the source of truth) and the Wallet Service's balance is a CDC projection of it (`transactions_db.transactions` → Kafka Connect → topic `transactions_db.transactions` → wallet consumer). The test suite is built around this fact:

- **Ledger-first assertions:** every balance check inside a test reads the `balance` / `balance_after` field of the Transactions Service's synchronous HTTP response, which is committed atomically with the ledger write. These assertions are deterministic.
- **Convergence assertions:** each test ends with `waitForBalance(t, phone, want)`, which polls the Wallet Service's `GET /v1/wallet/phone/{phone}` until the CDC pipeline has propagated the ledger state (250ms interval, 15s timeout). This verifies the full pipeline end-to-end and guarantees the *next* test starts from a fresh, non-stale wallet read.

> **Why not just read the wallet balance immediately after each deposit?** The wallet is a projection that lags the ledger (change stream → Connect offset flush → consumer). Immediate reads race the pipeline and flake. Reading it *twice* in a row (before and after a duplicate request) can even mask double-charges. Ledger-first + one convergence check per wallet avoids both traps.

## Prerequisites & Running

The suite needs the **full live stack**, not just the two HTTP services:

| Component | Why |
|---|---|
| Wallet Service (`:8000`) | Wallet creation + convergence polling |
| Transactions Service (`:8080`) | All ledger operations |
| Kafka broker + Kafka Connect (Mongo source) + wallet consumer | `waitForBalance` checks real CDC convergence — without them, every convergence check times out |

```bash
cd Transactions-Service/svc-transactions

# full run (600s timeout: each test ends with a CDC convergence wait)
go test -v -tags=integration -count=1 -timeout 600s ./tests/

# single test
go test -v -tags=integration -count=1 -timeout 600s ./tests/ -run TestAtomicity_Transfer
```

Or run `.\tests\run_tests.ps1` on Windows.

The tests are behind the `//go:build integration` build tag. CI (`.github/workflows/ci.yml`) runs plain `go test ./...`, which **skips** this suite — the tests need live services and must not run on the CI runner. The tag also means `go vet -tags=integration ./tests/` is needed to type-check the file locally.

> **Test order matters:** the suite is stateful and sequential. `TestSetup_CreateWallets` and `TestSetup_CreateTransferWallets` create/fund the wallets used by later tests, and balances accumulate across tests (see tables below). `go test` runs tests in file order within a package — do not shuffle. Running a single test requires its setup test to have run in a previous execution (wallets persist in Mongo).

**Concurrent helper safety:** `deposit` / `withdraw` / `transfer` use `t.Errorf` (never `t.Fatalf`) so they are safe to call from goroutines in the concurrency tests. `t.Fatalf` is reserved for the main test goroutine.

## Test Scenarios Overview

| Test Name | Concurrency | Purpose |
|-----------|-------------|---------|
| `TestSetup_CreateWallets` | 1 | Initializes the wallets required for testing. |
| `TestAtomicity_Deposit...` | 1 | Verifies the ledger response (`balance`, `status: POSTED`) and that the balance converges in the wallet via CDC. |
| `TestConsistency_Sequential...` | 1 | Verifies sequential math by chaining the `balance` field of each response; converges once at the end. |
| `TestIsolation_ConcurrentDeposits...` | 20 | Fires 20 simultaneous deposits. Asserts max(response balances) == baseline + successes×100, then converges. |
| `TestIsolation_ConcurrentWithdrawals...`| 20 | Fires 20 simultaneous withdrawals to ensure balance floors are respected under load. Asserts min(response balances) == baseline − successes×100. |
| `TestIsolation_ConcurrentMixed...` | 40 (20+20) | Fires deposits and withdrawals simultaneously on the same wallet; baseline taken from the funding response. |
| `TestDurability_IdempotencyOnTransactionService` | 1 | Submits identical deposit requests; retries must return the same `transaction_id` and the same `balance`. |
| `TestIsolation_ConcurrentIdempotentRequests` | 20 | 20 concurrent identical requests: all successes must show the same `transaction_id` and ledger balance. |
| `TestEdgeCase_WithdrawMore...` | 1 | Withdraw beyond balance is rejected; wallet must still converge to the pre-test balance. |
| `TestEdgeCase_DepositExceedsMax` | 1 | Tests the wallet balance ceiling; the top-up deposit response must report exactly `WalletMax`. |
| `TestHighConcurrency_50Deposits...` | 50 | The main stress test for the retry architecture. |
| `TestSetup_CreateTransferWallets` | 1 | Initializes sender & receiver wallets, pre-funds sender with 50,000, and waits for the sender balance to converge. |
| `TestAtomicity_Transfer` | 1 | Verifies the sender's `balance_after` from the response, then convergence of both sender and receiver. |
| `TestConsistency_TransferInsufficientBalance` | 1 | Transfer beyond sender balance fails; both wallets must converge unchanged. |
| `TestConsistency_TransferMaxBalanceFails` | 1 | Exceeding receiver max capacity aborts with 422; sender unchanged. Resets the receiver afterwards and verifies the reset converged. |
| `TestIsolation_ConcurrentTransfers` | 20 | 20 simultaneous transfers; asserts min(sender `balance_after`) == baseline − successes×50, then converges both wallets. |
| `TestDurability_IdempotentTransfer` | 1 | Retry returns the same `transaction_id` and identical `balance_after`; both wallets converge to a single execution. |
| `TestDurability_IdempotentFailedTransfer` | 1 | Retry of a 422-failed transfer returns the same error; the FAILED record is excluded from CDC, so neither wallet balance may change. Resets receiver afterwards. |
| `TestEdgeCase_TransferToSelf` | 1 | Self-transfer is rejected with 400; balance unchanged. |
| `TestIsolation_ConcurrentWithdrawalsOnlyOneSucceeds` | 20 | Funds the wallet, waits for CDC convergence, then reads the wallet balance **dynamically** (residual balance from previous runs is included — the test never assumes a clean wallet). Fires 20 concurrent full-balance withdrawals: exactly 1 must succeed (201) with response `balance: 0`, the other 19 must fail, and the wallet must converge to 0. |

> The wallet-side idempotency test (`TestDurability_IdempotencyOnWalletService`) was removed: it targeted the wallet service's `PATCH /v1/wallet/balance` endpoint, which no longer exists — all balance mutation lives in the Transactions Service.

## How the Code Works Under the Hood
Because the Transactions Service enforces a unique sequence number for each wallet transaction, concurrent transactions for the same wallet are serialized. 
If 20 goroutines fire at once, MongoDB's `session.WithTransaction()` isolates them. One wins, and the other 19 fail with a `WriteConflict`. The Go application code catches the `ErrDuplicateSequence` equivalent, applies a random jitter via `time.Sleep()`, and retries.

### Transfer Atomicity
Transfers create **two** transaction records inside a single MongoDB session transaction: a withdrawal record for the sender and a deposit record for the receiver. If the receiver's wallet would exceed max capacity, the entire MongoDB transaction aborts — neither record is created. A separate `StatusFailed` transaction is then recorded for the sender to preserve the audit trail.

### Idempotency in Transfers
The sender's withdrawal transaction carries the original `ReferenceID` (Idempotency-Key). The receiver's deposit transaction carries `ReferenceID + "_deposit"`. On retry, the service checks for an existing transaction with the sender's phone + reference ID. If found and `StatusCompleted`, it returns the cached response without re-executing.

### CDC & the `waitForBalance` contract
Only transactions with `status: POSTED` ride the bus (the Kafka Connect pipeline filters them), and the wallet consumer applies the event's `balance_after` to the wallet document. `FAILED` records never reach the wallet — which is why the failed-transfer tests assert that balances stay put. If `waitForBalance` times out, it means one of:

1. **CDC lag** > 15s (pipeline slow but healthy) — rerun/raise `convergeTimeout`.
2. **Pipeline down** (Kafka/Connect/consumer not running) — check prerequisites.
3. **Dropped or mis-ordered event** — a real correctness bug (e.g., an event whose wallet did not exist yet); this is the failure mode the convergence check exists to catch.

## Expected Database State 
If you run `go test -v -tags=integration -count=1 -timeout 600s ./tests/` on a **clean, empty database**, here is exactly how the database should look at the end of the test suite:

### `wallet_db.wallets` collection
- 7 wallet documents: `01012345678`, `01112345678`, `01212345678`, `01512345678`, `01055555555`, `01166666666`, `01027272727`

### `transactions_db.transactions` collection

| Phone Number | Description | Transaction Count |
|---|---|---|
| `01012345678` | Deposit/Withdraw tests + Idempotency + Stress | ~77 records |
| `01112345678` | Setup deposit + 20 concurrent withdrawals | ~21 records |
| `01212345678` | Setup deposit + 40 concurrent mixed ops | ~41 records |
| `01512345678` | Max capacity deposit (failed withdraw/deposit attempts create no records) | 1 record |
| `01027272727` | Race test: 1 deposit + 1 successful withdrawal (19 rejected). Re-runs add 2 more records each (the test reads the current wallet balance dynamically, so residual balance doesn't break it) | 2 records per run |
| `01055555555` | Sender: setup + transfers + concurrent + FAILED status records | Variable (~26) |
| `01166666666` | Receiver: transfer deposits + max capacity setup/reset | Variable (~25) |

> **Note:** The exact count for transfer wallets varies because `TestConsistency_TransferMaxBalanceFails` and `TestDurability_IdempotentFailedTransfer` each deposit to max and withdraw back, and the failed transfer creates a `StatusFailed` record on the sender. Counts for concurrency tests also shrink if some requests exhaust their OCC retries (429).

### Expected Final Balances (Clean Database)

Concurrent tests tolerate OCC retry exhaustion — the suite computes expected values as `baseline + successes × amount`, and balances converge to that. The table below shows the **best case** (all concurrent operations succeed):

| Phone Number | Starting Balance | Net Change | Expected Final Balance |
|---|---|---|---|
| `01012345678` | `0` | +1000 (Atomicity) <br> +3500 (Sequential) <br> +2000 (Concurrent Deposits) <br> +300 (Tx Idempotency) <br> +100 (Concurrent Idempotency) <br> +500 (Stress 50) | **7,400** |
| `01112345678` | `0` | +10000 (Setup) <br> -2000 (Concurrent Withdrawals) | **8,000** |
| `01212345678` | `0` | +50000 (Setup) <br> +2000 (Concurrent Mixed) <br> -2000 (Concurrent Mixed) | **50,000** |
| `01512345678` | `0` | +9000000000000000 (Max Capacity Deposit) | **9,000,000,000,000,000** |
| `01027272727` | `0` | +5000 (Race Deposit) <br> -5000 (1 Successful Withdrawal) | **0** |
| `01055555555` | `0` | +50000 (Setup) <br> -500 (Atomicity) <br> +0 (Insufficient Fail) <br> +0 (Max Capacity Fail, sender gets FAILED tx) <br> +5000 (Concurrent Setup) <br> -1000 (Concurrent 20×50) <br> -100 (Idempotent Transfer) <br> +0 (Idempotent Failed Transfer) <br> +0 (Self-Transfer Reject) | **53,400** |
| `01166666666` | `0` | +500 (Atomicity) <br> +0 (Insufficient Fail) <br> +0 (Max Capacity resets) <br> +1000 (Concurrent 20×50) <br> +100 (Idempotent Transfer) <br> +0 (Idempotent Failed resets) | **1,600** |

If any concurrent operations fail with 429 (retry exhaustion), subtract `failed × amount` from the affected wallet's final balance — the suite itself passes as long as the wallet balance matches `baseline + successes × amount`, because the ledger and the wallet stay consistent with each other.
