package iotmaker_db_mongodb

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DbFunctions struct {
	Client      interface{}
	collections map[string]interface{}
	dbString    string
}

//"mongodb://0.0.0.0:27017"
func (el *DbFunctions) Connect(connection ...interface{}) error {
	var err error
	var connString string
	var dbString string
	var collectionsList []string

	if len(connection) != 3 {
		return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
	}

	switch connection[0].(type) {
	case string:
		connString = connection[0].(string)

		if connString == "" {
			return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
		}

	default:
		return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
	}

	switch connection[1].(type) {
	case string:
		dbString = connection[1].(string)

		if dbString == "" {
			return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
		}

		el.dbString = dbString

	default:
		return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
	}

	switch connection[2].(type) {
	case []string:
		collectionsList = connection[2].([]string)

		if len(collectionsList) == 0 {
			return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
		}

		el.collections = make(map[string]interface{})

	default:
		return errors.New("connection must be a string like 'mongodb://0.0.0.0:27017', 'server_name', []string{collections_list}")
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(connString)

	// Connect to MongoDB
	el.Client, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}

	// Check the connection
	err = el.Client.(*mongo.Client).Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	for _, collectionName := range collectionsList {
		el.collections[collectionName] = el.Client.(*mongo.Client).Database(el.dbString).Collection(collectionName)
	}

	return nil
}

func (el *DbFunctions) Disconnect() error {
	return el.Client.(*mongo.Client).Disconnect(context.TODO())
}

func (el *DbFunctions) Find(collection, query interface{}, pointerToResult *[]map[string]interface{}) error {
	var err error
	var cursor *mongo.Cursor
	var toDecode = make(map[string]interface{})

	coll := collection.(string)
	if el.collections[coll] == false {
		return errors.New("it appears that you do not have permission to access this collection or the name of it has been entered wrong. collection name: " + coll)
	}

	cursor, err = el.collections[coll].(*mongo.Collection).Find(context.TODO(), query)

	if cursor == nil {
		return errors.New("mongodb.find().error: cursor is nil")
	}

	if err = cursor.Err(); err != nil {
		return err
	}

	for cursor.Next(context.TODO()) {
		err = cursor.Decode(&toDecode)
		if err != nil {
			return err
		}

		*pointerToResult = append(*pointerToResult, toDecode)
	}

	return cursor.Close(context.TODO())
}

func (el *DbFunctions) Count(collection, query interface{}) (error, int64) {
	var err error
	var count int64

	coll := collection.(string)
	if el.collections[coll] == false {
		return errors.New("it appears that you do not have permission to access this collection or the name of it has been entered wrong. collection name: " + coll), 0
	}

	count, err = el.collections[coll].(*mongo.Collection).CountDocuments(context.TODO(), query)

	return err, count
}

func (el *DbFunctions) Insert(collection, data interface{}) error {
	var err error

	coll := collection.(string)
	if el.collections[coll] == false {
		return errors.New("it appears that you do not have permission to access this collection or the name of it has been entered wrong. collection name: " + coll)
	}

	_, err = el.collections[coll].(*mongo.Collection).InsertOne(context.TODO(), data)

	return err
}
