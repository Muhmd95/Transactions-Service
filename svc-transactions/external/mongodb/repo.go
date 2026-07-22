package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	//project imports
	"svc-transactions/internal/transactions"
)

type mongoRepository struct {
	collection *mongo.Collection
}

func NewTransactionRepository(ctx context.Context, db *mongo.Database) (transactions.Repository, error) {
	coll := db.Collection("transactions")

	// config the database by the reference id to create idempotency
	//_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{ // creating an index on reference_, passing ctx to track the time
	//Keys:    bson.M{"reference_id": 1},                                   // this is the index on the reference ID field, 1 means ascending order
	//Options: options.Index().SetUnique(true).SetName("unique_reference"), // this is the name of the index and it is unique so
	//that no two transactions can have the same reference ID
	//})
	//if err != nil {
	//return nil, err
	//}
	return &mongoRepository{collection: coll}, nil
}

func (r *mongoRepository) CreateTransaction(ctx context.Context, transaction *transactions.Transaction) error {

	result, err := r.collection.InsertOne(ctx, transaction)
	if err != nil {
		//if mongo.IsDuplicateKeyError(err) {
		//return transactions.ErrDuplicateReferenceID
		//}
		return err
	}
	transaction.ID = result.InsertedID.(primitive.ObjectID)

	return nil
}

func (r *mongoRepository) UpdateTransactionStatus(ctx context.Context, transactionID primitive.ObjectID, status transactions.TransactionStatus, failedReason string) error {
	// apllied filter
	filter := bson.M{"_id": transactionID}
	// update the status field
	setUpdate := bson.M{
		"status":     status,
		"updated_at": time.Now(),
	}
	if failedReason != "" {
		setUpdate["failed_reason"] = failedReason
	}

	update := bson.M{"$set": setUpdate}

	_, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	return nil

}
