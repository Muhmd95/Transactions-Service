package rest

import (
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
	_ "svc-transactions/docs"
)

// @title           Transactions Service API
// @version         1.0
// @description     This microservice handles financial transactions.
// @host            localhost:8080
// @BasePath        /v1
func RegisterRoutes(mux *http.ServeMux, controller *TransactionsController) {

	depositHandler := otelhttp.NewHandler(http.HandlerFunc(controller.DepositHandler), "DepositHandler")

	// 1. deposit handler
	mux.Handle("POST /v1/transactions/deposit", depositHandler)

	withdrawHandler := otelhttp.NewHandler(http.HandlerFunc(controller.WithdrawHandler), "WithdrawHandler")
	// 2. withdraw handler
	mux.Handle("POST /v1/transactions/withdraw", withdrawHandler)

	// 3. transfer handler
	transferHandler := otelhttp.NewHandler(http.HandlerFunc(controller.TransferHandler), "TransferHandler")
	mux.Handle("POST /v1/transactions/transfer", transferHandler)

	// 4. Swagger UI handler mounted directly to your mux
	mux.HandleFunc("/v1/swagger/", httpSwagger.Handler(
		httpSwagger.URL("/v1/swagger/doc.json"),
	))
}
