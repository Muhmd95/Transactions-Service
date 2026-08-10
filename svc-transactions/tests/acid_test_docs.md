# Transactions Service ACID Test Documentation

This document describes the `acid_test.go` integration tests for the Transactions Service. These tests hit the live HTTP endpoints to verify Atomicity, Consistency, Isolation, and Durability (ACID) properties of the transaction flows, especially focusing on how MongoDB handles concurrent operations.

## Test Scenarios Overview

| Test Name | Concurrency | Purpose |
|-----------|-------------|---------|
| `TestSetup_CreateWallets` | 1 | Initializes the wallets required for testing. |
| `TestAtomicity_Deposit...` | 1 | Verifies that a transaction is posted and the wallet balance is updated synchronously. |
| `TestConsistency_Sequential...` | 1 | Verifies standard sequential math over a series of mixed operations. |
| `TestIsolation_ConcurrentDeposits...` | 20 | Fires 20 simultaneous deposits. Tests the optimistic concurrency retry loop. |
| `TestIsolation_ConcurrentWithdrawals...`| 20 | Fires 20 simultaneous withdrawals to ensure balance floors are respected under load. |
| `TestIsolation_ConcurrentMixed...` | 40 (20+20) | Fires deposits and withdrawals simultaneously on the same wallet. |
| `TestDurability_Idempotency...` | 1 | Bypasses the transaction service to test wallet idempotency directly. |
| `TestEdgeCase_WithdrawMore...` | 1 | Tests that withdrawing beyond the balance limit throws a 400 error. |
| `TestEdgeCase_DepositExceedsMax` | 1 | Tests the theoretical wallet balance ceiling limit. |
| `TestHighConcurrency_50Deposits...` | 50 | The main stress test for the retry architecture. |

## How the Code Works Under the Hood
Because the Transactions Service enforces a unique sequence number for each wallet transaction, concurrent transactions for the same wallet are serialized. 
If 20 goroutines fire at once, MongoDB's `session.WithTransaction()` isolates them. One wins, and the other 19 fail with a `WriteConflict`. The Go application code catches the `ErrDuplicateSequence` equivalent, applies a random jitter via `time.Sleep()`, and retries.

## Expected Database State 
If you run `go test -v -tags=integration -count=1 ./tests/` on a **clean, empty database**, here is exactly how the database should look at the end of the test suite:

### `transactions_db.transactions` collection
- **Phone `01012345678`**: 76 transaction records (Seq 1 through 76)
- **Phone `01112345678`**: 21 transaction records (Seq 1 through 21)
- **Phone `01212345678`**: 41 transaction records (Seq 1 through 41)
- **Phone `01512345678`**: 1 transaction record (The massive setup deposit)

### Expected Final Balances

| Phone Number | Starting Balance | Net Change Through Tests | Expected Final Balance |
|--------------|------------------|--------------------------|------------------------|
| `01012345678`| `0` | +1000 (Test2) <br> +3500 (Test3) <br> +2000 (Test4) <br> +150 (Test7) <br> +500 (Test10) | **7,150** |
| `01112345678`| `0` | +10000 (Setup) <br> -2000 (Test5) | **8,000** |
| `01212345678`| `0` | +50000 (Setup) <br> +2000 (Test6) <br> -2000 (Test6) | **50,000** |
| `01512345678`| `0` | +9000000000000000 (Test9 Setup) | **9,000,000,000,000,000** |
