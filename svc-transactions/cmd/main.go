package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
    "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	// project paths
	"svc-transactions/api/rest"
	"svc-transactions/client/wallet"
	"svc-transactions/external/mongodb"
	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
	"svc-transactions/util/tracer"
)

func main() {
	// inti the logger
	logger.InitLogger("svc-transactions")
	logger.Log.Info().Msg("Starting svc-transactions")

	// init rhe tracer
	tp, err := tracer.InitTracer("svc-transactions")
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to initialize the tracer")

	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to shutdown tracer")
		}
	}()

	// loading the .ENV
	if err := godotenv.Load(".ENV"); err != nil {
		logger.Log.Info().Msg("No .ENV file found, relying on os environment")
	}

	// connect the port
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	// get mongo uri
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		logger.Log.Fatal().Msg("MONGO_URI environment variable missing")

	}
	// GET DBNAME
	dbName := os.Getenv("MONGO_DB_NAME")
	if dbName == "" {
		dbName = "transactions_db"
	}

	// GET wallet client url
	walletTarget := os.Getenv("WALLET_GRPC_URL")
	if walletTarget == "" {
		walletTarget = "localhost:50051"
	}

	mongoClient, err := mongodb.ConnectMongoDB(mongoURI)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to connect to the database")

	}

	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to diconnect the database")
		}
	}()

	// init the database
	database := mongoClient.Database(dbName)
	logger.Log.Info().Str("dbName", dbName).Msg("Using database")

	transactionsRepo, err := mongodb.NewTransactionRepository(context.Background(), database)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to create transactions repository")
	}
	// init the client
	conn, err := grpc.NewClient(
		walletTarget,
		// Use insecure credentials because it is internal connection
		// it is just for this project to reduce cpu usage and complexity
        grpc.WithTransportCredentials(insecure.NewCredentials()),

		// this is the interceptor that will grab the trace id and inject it 
		// to the context of any grpc outgoing request
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to connect to WalletService")
	}
	defer conn.Close()

	walletClient := wallet.NewWalletClient(conn)
	//walletClient := wallet.NewWalletClient(walletBaseURL)
	// init the service
	service := transactions.NewService(transactionsRepo, walletClient)
	// init the controller
	controller := rest.NewTransactionsController(service)

	mux := http.NewServeMux()

	rest.RegisterRoutes(mux, controller)

	// start the http server
	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// ListenAndServe blocks forever unless it crashes
	//if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	//logger.Log.Fatal().Err(err).Msg("Server crashed")

	//}

	// from gemini copy paste
	// 1. Run the server in a goroutine
	go func() {
		logger.Log.Info().Str("port", port).Msg("Server is listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal().Err(err).Msg("Server crashed")
		}
	}()

	// 2. Set up the signal listener
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// 3. Block until a shutdown signal is caught
	<-quit
	logger.Log.Info().Msg("Shutting down svc-transactions gracefully...")

	// 4. Wait up to 10 seconds for current requests to finish
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Log.Fatal().Err(err).Msg("Server forced to shutdown")
	}

	logger.Log.Info().Msg("svc-transactions exited safely")
}
