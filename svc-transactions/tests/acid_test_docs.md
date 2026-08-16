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
| `TestDurability_IdempotencyOnTransactionService` | 1 | Tests idempotency on the Transactions Service by submitting identical deposit requests. |
| `TestIsolation_ConcurrentIdempotentRequests` | 20 | Tests idempotency on the Transactions Service under high concurrency. |
| `TestEdgeCase_WithdrawMore...` | 1 | Tests that withdrawing beyond the balance limit throws a 400 error. |
| `TestEdgeCase_DepositExceedsMax` | 1 | Tests the theoretical wallet balance ceiling limit. |
| `TestHighConcurrency_50Deposits...` | 50 | The main stress test for the retry architecture. |
| `TestSetup_CreateTransferWallets` | 1 | Initializes sender & receiver wallets and pre-funds sender with 50,000. |
| `TestAtomicity_Transfer` | 1 | Verifies transfer deducts from sender and credits receiver atomically. |
| `TestConsistency_TransferInsufficientBalance` | 1 | Ensures transferring more than sender balance fails and both wallets are unchanged. |
| `TestConsistency_TransferMaxBalanceFails` | 1 | Ensures exceeding receiver max capacity safely aborts, sender is not charged. |
| `TestIsolation_ConcurrentTransfers` | 20 | Fires 20 simultaneous transfers to test concurrency on both sender and receiver. |
| `TestDurability_IdempotentTransfer` | 1 | Verifies retry of successful transfer returns same transaction ID, balance only changes once. |
| `TestDurability_IdempotentFailedTransfer` | 1 | Verifies retry of failed transfer returns same 422, sender balance unchanged. |
| `TestEdgeCase_TransferToSelf` | 1 | Ensures transferring to yourself is rejected with 400 Bad Request. |

## How the Code Works Under the Hood
Because the Transactions Service enforces a unique sequence number for each wallet transaction, concurrent transactions for the same wallet are serialized. 
If 20 goroutines fire at once, MongoDB's `session.WithTransaction()` isolates them. One wins, and the other 19 fail with a `WriteConflict`. The Go application code catches the `ErrDuplicateSequence` equivalent, applies a random jitter via `time.Sleep()`, and retries.

### Transfer Atomicity
Transfers create **two** transaction records inside a single MongoDB session transaction: a withdrawal record for the sender and a deposit record for the receiver. If the receiver's wallet would exceed max capacity, the entire MongoDB transaction aborts — neither record is created. A separate `StatusFailed` transaction is then recorded for the sender to preserve the audit trail.

### Idempotency in Transfers
The sender's withdrawal transaction carries the original `ReferenceID` (Idempotency-Key). The receiver's deposit transaction carries `ReferenceID + "_deposit"`. On retry, the service checks for an existing transaction with the sender's phone + reference ID. If found and `StatusCompleted`, it returns the cached response without re-executing.

## Expected Database State 
If you run `go test -v -tags=integration -count=1 ./tests/` on a **clean, empty database**, here is exactly how the database should look at the end of the test suite:

### `wallet_db.wallets` collection
- 6 wallet documents: `01012345678`, `01112345678`, `01212345678`, `01512345678`, `01055555555`, `01166666666`

### `transactions_db.transactions` collection

| Phone Number | Description | Transaction Count |
|---|---|---|
| `01012345678` | Deposit/Withdraw tests + Idempotency + Stress | ~78 records |
| `01112345678` | Setup deposit + 20 concurrent withdrawals | ~21 records |
| `01212345678` | Setup deposit + 40 concurrent mixed ops | ~41 records |
| `01512345678` | Max capacity setup deposit | 1 record |
| `01055555555` | Sender: setup + transfers + concurrent + failed (FAILED status records included) | Variable (~28+) |
| `01166666666` | Receiver: transfer deposits + max capacity setup/reset | Variable (~28+) |

> **Note:** The exact count for transfer wallets varies because `TestConsistency_TransferMaxBalanceFails` and `TestDurability_IdempotentFailedTransfer` each deposit to max and withdraw back, and the failed transfer creates a `StatusFailed` record on the sender.

### Expected Final Balances (Clean Database)

| Phone Number | Starting Balance | Net Change | Expected Final Balance |
|---|---|---|---|
| `01012345678` | `0` | +1000 (Atomicity) <br> +3500 (Sequential) <br> +2000 (Concurrent Deposits) <br> +150 (Wallet Idempotency) <br> +300 (Tx Idempotency) <br> +100 (Concurrent Idempotency) <br> +500 (Stress 50) | **7,550** |
| `01112345678` | `0` | +10000 (Setup) <br> -2000 (Concurrent Withdrawals) | **8,000** |
| `01212345678` | `0` | +50000 (Setup) <br> +2000 (Concurrent Mixed) <br> -2000 (Concurrent Mixed) | **50,000** |
| `01512345678` | `0` | +9000000000000000 (Max Capacity Setup) | **9,000,000,000,000,000** |
| `01055555555` | `0` | +50000 (Setup) <br> -500 (Atomicity) <br> +0 (Insufficient Fail) <br> +0 (Max Capacity Fail, sender gets FAILED tx) <br> +5000 (Concurrent Setup) <br> -1000 (Concurrent 20×50) <br> -100 (Idempotent Transfer) <br> +0 (Idempotent Failed Transfer) <br> +0 (Self-Transfer Reject) | **53,400** |
| `01166666666` | `0` | +500 (Atomicity) <br> +0 (Insufficient Fail) <br> +0 (Max Capacity resets to 0) <br> +1000 (Concurrent 20×50) <br> +100 (Idempotent Transfer) <br> +0 (Idempotent Failed resets to 0) | **1,600** |
