package mongodb

import (
	"context"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	//project imports
	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)

type mongoRepository struct {
	collection *mongo.Collection
}

func NewTransactionRepository(ctx context.Context, db *mongo.Database) (transactions.Repository, error) {
	coll := db.Collection("transactions")
	log := logger.Ctx(ctx)

	// create a compound index of phone number and the sequence number
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{ // compound index, bson.D is for the order doc -> phone number then the sequence
			{Key: "phone_number", Value: 1},
			{Key: "sequence_number", Value: -1},
		},
		Options: options.Index().SetUnique(true).SetName("unique_wallet_sequence"),
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create the unique sequence index (from repo layer)")
		return nil, err
	}

	// config the database by the reference id to create idempotency
	_, err = coll.Indexes().CreateOne(ctx, mongo.IndexModel{ // creating an index on reference_, passing ctx to track the time
	Keys:    bson.M{"reference_id": 1},                                   // this is the index on the reference ID field, 1 means ascending order
	Options: options.Index().SetUnique(true).SetName("unique_reference"), // this is the name of the index and it is unique so
	// that no two transactions can have the same reference ID
	})
	if err != nil {
	return nil, err
	}
	return &mongoRepository{collection: coll}, nil
}

func (r *mongoRepository) CreateTransaction(ctx context.Context, transaction *transactions.Transaction) error {

	log := logger.Ctx(ctx)

	result, err := r.collection.InsertOne(ctx, transaction)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			if strings.Contains(err.Error(), "unique_reference") {
				return transactions.ErrDuplicateReferenceID
			}
			if strings.Contains(err.Error(), "unique_wallet_sequence") {
				return transactions.ErrDuplicateSequence
			}
		}
		if strings.Contains(err.Error(), "WriteConflict") {
			return transactions.ErrDuplicateSequence // to make the loop retry after a bit of time
		}
		log.Error().Err(err).Msg("Failed to insert transaction (from repo layer)")
		return err
	}
	transaction.ID = result.InsertedID.(primitive.ObjectID)

	return nil
}

func (r *mongoRepository) GetLatestTransaction(ctx context.Context, PhoneNumber string) (*transactions.Transaction, error) {
	var latestTX transactions.Transaction

	filter := bson.M{"phone_number": PhoneNumber}

	option := options.FindOne().SetSort(bson.D{{Key: "sequence_number", Value: -1}})

	err := r.collection.FindOne(ctx, filter, option).Decode(&latestTX)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, transactions.ErrFirstTransaction
		}
		return nil, err
	}
	return &latestTX, nil
}

func (r *mongoRepository) GetTransactionByReferenceID(ctx context.Context, referenceID string, optionalPhoneNNumber *string) (*transactions.Transaction, error) {
	var transaction transactions.Transaction

	filter := bson.M{"reference_id": referenceID}

	if optionalPhoneNNumber != nil {
		filter["phone_number"] = *optionalPhoneNNumber
	}

	err := r.collection.FindOne(ctx, filter).Decode(&transaction)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, transactions.ErrTransactionNotFound
		}
		return nil, err
	}
	return &transaction, nil
}

// func (r *mongoRepository) UpdateTransactionStatus(ctx context.Context, transactionID primitive.ObjectID, status transactions.TransactionStatus, failedReason string) error {
// 	log := logger.Ctx(ctx)
// 	// apllied filter
// 	filter := bson.M{"_id": transactionID}
// 	// update the status field
// 	setUpdate := bson.M{
// 		"status":     status,
// 		"updated_at": time.Now(),
// 	}
// 	if failedReason != "" {
// 		setUpdate["failed_reason"] = failedReason
// 	}

// 	update := bson.M{"$set": setUpdate}

// 	_, err := r.collection.UpdateOne(ctx, filter, update)
// 	if err != nil {
// 		log.Error().Err(err).Msg("Failed to update the transaction status (from repo layer)")
// 		return err
// 	}
// 	return nil

// }
