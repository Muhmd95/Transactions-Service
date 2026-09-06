package mongodb

import (
	"context"
	"svc-transactions/internal/transactions"

	"go.mongodb.org/mongo-driver/mongo"
)

type mongoTxManager struct {
	client *mongo.Client
}

func NewTransactionsTxManager(clt *mongo.Client) transactions.TxManager {
	return &mongoTxManager{client: clt}
}

func (t *mongoTxManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	// start a new session
	session, err := t.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx) // pass the ctx because it is a network call

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// here i will call the function from the service but pass the session context to it
		// because mongo session context implements the context interface the fn takes it
		err := fn(sessCtx)

		// return the error from my fn
		// fn returns nil if success then the with transction commit the transantion
		// else it retry it or abort
		// if duplicate documen t will handle it in the for loop of service
		return nil, err

	})

	return err
}
