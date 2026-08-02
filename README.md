# 💸 Transactions Service (`svc-transactions`)

> **Status:** 🚧 Work in Progress — this service is part of a larger Wallet & Transactions microservice system and is not yet complete.

## Overview

The Transactions Service is a Go microservice responsible for orchestrating financial transactions (deposits and withdrawals). It exposes a **REST API**, persists transaction records in **MongoDB**, and communicates with the **Wallet Service** over **gRPC** to execute balance modifications. The service includes **OpenTelemetry** tracing for distributed observability across service boundaries.

## Architecture

```
svc-transactions/
├── cmd/                     # Application entry point
│   └── main.go
├── api/                     # Transport / delivery layer
│   └── rest/                # REST API (HTTP handlers, routes, controller)
├── internal/
│   └── transactions/        # Core domain (model, service, DTOs, repository interface)
├── external/
│   └── mongodb/             # Repository implementation (MongoDB)
├── client/
│   └── wallet/              # gRPC client for the Wallet Service
├── util/
│   ├── logger/              # Structured logging (zerolog + trace context)
│   └── tracer/              # OpenTelemetry tracer setup
├── docs/                    # Swagger / OpenAPI auto-generated docs
├── Dockerfile               # Multi-stage Docker build
└── go.mod
```

The project follows a **clean-architecture** style layout:

| Layer | Package | Responsibility |
|-------|---------|----------------|
| **Transport** | `api/rest` | Handle HTTP requests, validate input, map domain errors to HTTP status codes |
| **Domain** | `internal/transactions` | Business logic, domain model, DTOs, repository & client interfaces |
| **Infrastructure** | `external/mongodb` | MongoDB implementation of the repository interface |
| **Client** | `client/wallet` | gRPC client that calls the Wallet Service's `ModifyBalance` RPC |
| **Utilities** | `util/logger`, `util/tracer` | Cross-cutting concerns (logging, tracing) |

## Tech Stack

- **Language:** Go 1.26
- **Database:** MongoDB
- **REST Framework:** Go standard library (`net/http`)
- **Inter-service Communication:** gRPC client → Wallet Service (via `github.com/Muhmd95/Contracts`)
- **Logging:** [zerolog](https://github.com/rs/zerolog) with console writer
- **Tracing:** OpenTelemetry SDK (with `otelgrpc` and `otelhttp` instrumentation)
- **API Docs:** Swagger via [swaggo](https://github.com/swaggo/swag)
- **Containerization:** Docker (multi-stage Alpine build)

## How It Works

Each transaction follows a **two-phase** flow:

1. **Create a PENDING transaction** — a record is inserted into MongoDB with status `PENDING`.
2. **Call the Wallet Service** — a gRPC `ModifyBalance` request is sent to credit or debit the wallet.
   - ✅ **Success →** the transaction is updated to `COMPLETED`.
   - ❌ **Failure →** the transaction is updated to `FAILED` with the error reason stored.

The gRPC client maps Wallet Service error codes back to domain errors so the service layer stays decoupled from transport details.

## API Endpoints

### REST (default port `8080`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/transactions/deposit` | Process a deposit transaction |
| `POST` | `/v1/transactions/withdraw` | Process a withdrawal transaction |
| `GET` | `/v1/swagger/` | Swagger UI |

> Phone numbers must follow the Egyptian format: `+20XXXXXXXXXX` (13 characters total).

## Transaction Model

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectID | Auto-generated MongoDB ID |
| `sender_phone` | string | Phone number of the sender (used in withdrawals) |
| `receiver_phone` | string | Phone number of the receiver (used in deposits) |
| `type` | string | `DEPOSIT`, `WITHDRAWAL`, or `TRANSFER` |
| `status` | string | `PENDING`, `COMPLETED`, or `FAILED` |
| `amount` | int64 | Transaction amount in minor currency units |
| `failed_reason` | string | Error message when status is `FAILED` |
| `created_at` | timestamp | Record creation time |
| `updated_at` | timestamp | Last update time |

## Getting Started

### Prerequisites

- Go 1.26+
- A running MongoDB instance (local or Atlas)
- A running Wallet Service instance (gRPC on port `50051`)

### Configuration

Copy the example env file and fill in your values:

```bash
cp .ENV.example .ENV
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGO_URI` | ✅ | — | MongoDB connection string |
| `MONGO_DB_NAME` | ❌ | `transactions_db` | Database name |
| `SERVER_PORT` | ❌ | `8080` | HTTP server port |
| `WALLET_GRPC_URL` | ❌ | `localhost:50051` | Wallet Service gRPC address |

### Run Locally

```bash
cd svc-transactions
go run ./cmd
```

### Run with Docker

```bash
cd svc-transactions
docker build -t transactions-service .
docker run -p 8080:8080 \
  -e MONGO_URI="your_mongo_uri" \
  -e MONGO_DB_NAME="transactions_db" \
  -e WALLET_GRPC_URL="host.docker.internal:50051" \
  transactions-service
```

## Domain Errors

| Error | Meaning |
|-------|---------|
| `wallet not found` | No wallet exists for the given phone number |
| `invalid phone number format` | Phone number doesn't match `+20XXXXXXXXXX` |
| `insufficient balance for the requested operation` | Withdrawal would result in a negative balance |
| `deposit exceeds maximum wallet capacity` | Deposit would overflow `int64` max |
| `invalid transaction type` | Unrecognized transaction type |

## Roadmap

- [ ] Transfer transactions (wallet-to-wallet)
- [ ] Idempotency via `reference_id` unique index
- [ ] Currency code support on transactions
- [ ] Transaction history / query endpoints
- [ ] OpenTelemetry exporter (Kibana / Jaeger)
- [ ] Saga pattern for multi-step transaction rollbacks
