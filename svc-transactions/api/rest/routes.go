package rest

import (
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"net/http"
)

func RegisterRoutes(mux *http.ServeMux, controller *TransactionsController) {

	depositHandler := otelhttp.NewHandler(http.HandlerFunc(controller.DepositHandler), "DepositHandler")

	// 1. deposit handler
	mux.Handle("/v1/transactions/deposit", depositHandler)

	withdrawHandler := otelhttp.NewHandler(http.HandlerFunc(controller.WithdrawHandler), "WithdrawHandler")
	// 2. withdraw handler
	mux.Handle("/v1/transactions/withdraw", withdrawHandler)
}
