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
		if mongo.IsDuplicateKeyError(err) { // writeConflict or writeException included
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

func (r *mongoRepository) CreateCoupledTransaction(ctx context.Context, deposit *transactions.Transaction, withdrawal *transactions.Transaction) error {
	log := logger.Ctx(ctx)

	result, err := r.collection.InsertMany(ctx, []interface{}{deposit, withdrawal})
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
	deposit.ID = result.InsertedIDs[0].(primitive.ObjectID)
	withdrawal.ID = result.InsertedIDs[1].(primitive.ObjectID)

	return nil
}

func (r *mongoRepository) GetLatestTransaction(ctx context.Context, PhoneNumber string) (*transactions.Transaction, error) {
	// Only fetch fields the service actually uses
	projection := bson.M{
		"sequence_number": 1,
		"balance_after":   1,
		"wallet_id":       1,
		"_id":             1,
	}

	option := options.FindOne().
		SetSort(bson.D{{Key: "sequence_number", Value: -1}}).
		SetProjection(projection)

	var latestTX struct {
		ID           primitive.ObjectID `bson:"_id"`
		SeqNumber    int64              `bson:"sequence_number"`
		BalanceAfter int64              `bson:"balance_after"`
		WalletID     string             `bson:"wallet_id"`
	}

	err := r.collection.FindOne(ctx, bson.M{"phone_number": PhoneNumber}, option).Decode(&latestTX)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, transactions.ErrFirstTransaction
		}
		return nil, err
	}
	return &transactions.Transaction{
		ID:           latestTX.ID,
		SeqNumber:    latestTX.SeqNumber,
		BalanceAfter: latestTX.BalanceAfter,
		WalletID:     latestTX.WalletID,
	}, nil
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

func (r *mongoRepository) EarlyCheck(ctx context.Context, refID, phone string) (*transactions.Transaction, *transactions.Transaction, error) {
	log := logger.Ctx(ctx)
	pipeline := mongo.Pipeline{
		{{Key: "$facet", Value: bson.D{
			// Sub-pipeline 1: idempotency check by reference_id
			{Key: "byRef", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.M{
					"reference_id": refID,
					"phone_number": phone, // covers unique index fully
				}}},
				bson.D{{Key: "$limit", Value: 1}},
				bson.D{{Key: "$project", Value: bson.M{
					"_id": 1, "wallet_id": 1,
					"balance_after": 1, "balance_before": 1,
					"status": 1, "reference_id": 1,
					"created_at": 1, "type": 1,
				}}},
			}},

			// Sub-pipeline 2: latest transaction by sequence
			{Key: "latest", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.M{"phone_number": phone}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "sequence_number", Value: -1}}}},
				bson.D{{Key: "$limit", Value: 1}},
				bson.D{{Key: "$project", Value: bson.M{
					"_id": 1, "wallet_id": 1,
				}}},
			}},
		}}},
	}

	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute aggregation pipeline (from repo layer)")
		return nil, nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		ByRef  []transactions.Transaction `bson:"byRef"`
		Latest []transactions.Transaction `bson:"latest"`
	}
	if !cursor.Next(ctx) {
		return nil, nil, errors.New("no aggregation result")
	}
	if err := cursor.Decode(&result); err != nil {
		log.Error().Err(err).Msg("Failed to decode aggregation result (from repo layer)")
		return nil, nil, err
	}

	if len(result.ByRef) > 0 {
		return &result.ByRef[0], nil, nil // idempotent hit
	}
	if len(result.Latest) > 0 {
		return nil, &result.Latest[0], nil // latest for seq/balance
	}
	return nil, nil, transactions.ErrFirstTransaction
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
