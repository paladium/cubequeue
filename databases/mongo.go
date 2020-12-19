package databases

import (
	"context"

	"github.com/paladium/cubequeue/models"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TransactionMongoDBDatabase implementation of ITransactionDatabase using mongodb database
type TransactionMongoDBDatabase struct {
	client                 *mongo.Client
	db                     *mongo.Database
	transactionsCollection *mongo.Collection
}

// NewTransactionMongoDBDatabase connects to the database and finds the nessesary collection for storing transactions
func NewTransactionMongoDBDatabase(url string, db string, collection string) (*TransactionMongoDBDatabase, error) {
	database := TransactionMongoDBDatabase{}
	var err error
	database.client, err = mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		return nil, errors.Wrap(err, "Cannot connect to the database")
	}
	err = database.client.Connect(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "Cannot connect to the database")
	}
	database.db = database.client.Database(db)
	database.transactionsCollection = database.db.Collection(collection)
	return &database, nil
}

// Find a given model in mongodb database
func (database *TransactionMongoDBDatabase) Find(id string) (*models.TransactionModel, error) {
	result := database.transactionsCollection.FindOne(context.Background(), bson.M{"_id": id})
	if result.Err() != nil {
		return nil, result.Err()
	}
	transaction := new(models.TransactionModel)
	err := result.Decode(transaction)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot decode the model")
	}
	return transaction, nil
}

// Create saves the transaction in db
func (database *TransactionMongoDBDatabase) Create(transaction *models.TransactionModel) (*models.TransactionModel, error) {
	_, err := database.transactionsCollection.InsertOne(context.Background(), transaction)
	if err != nil {
		return nil, err
	}
	return transaction, nil
}

// Update updates the transaction in db
func (database *TransactionMongoDBDatabase) Update(id string, transaction *models.TransactionModel) (*models.TransactionModel, error) {
	_, err := database.transactionsCollection.UpdateOne(
		context.Background(),
		bson.M{"_id": id},
		bson.M{
			"$set": transaction,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot update the model")
	}
	return database.Find(id)
}

// Close the connection to the database
func (database *TransactionMongoDBDatabase) Close() {
	database.client.Disconnect(context.Background())
}

// DeleteDatabase drops the database
func (database *TransactionMongoDBDatabase) DeleteDatabase() error {
	err := database.db.Drop(context.Background())
	if err != nil {
		return errors.Wrap(err, "Cannot delete the database")
	}
	return nil
}
