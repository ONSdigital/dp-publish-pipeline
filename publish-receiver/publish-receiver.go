package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DataSet struct {
	CollectionId string `json:"collectionId"`
	FileLocation string `json:"fileLocation"`
	FileContent  string `json:"fileContent"`
	S3Location   string `json:"s3Location"`
}

type ReleaseCollection struct {
	CollectionId string `json:"collectionId"`
}

type S3Set struct {
	CollectionId string `bson:"collectionId"`
	FileLocation string `bson:"fileLocation"`
	S3Location   string `bson:"s3Location"`
}

type MetaSet struct {
	CollectionId string `bson:"collectionId"`
	FileLocation string `bson:"fileLocation"`
	FileContent  string `bson:"fileContent"`
}

const DATA_BASE = "onswebsite"
const MASTER_COLLECTION = "meta"
const S3_COLLECTION = "s3"

const MONGODB_ENV = "MONGODB"
const FILE_COMPLETE_TOPIC_ENV = "FILE_COMPLETE_TOPIC"

func storeData(jsonMessage []byte, db *mgo.Database) {
	var dataSet DataSet
	err := json.Unmarshal(jsonMessage, &dataSet)
	if err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if dataSet.CollectionId == "" || dataSet.FileLocation == "" {
		log.Printf("Unknown data from %v", dataSet)
		return
	}
	if dataSet.S3Location != "" {
		collection := db.C(S3_COLLECTION)
		s3Set := S3Set{dataSet.CollectionId, dataSet.FileLocation, dataSet.S3Location}
		addS3Document(*collection, s3Set)
	} else if dataSet.FileContent != "" {
		collection := db.C(MASTER_COLLECTION)
		metaSet := MetaSet{dataSet.CollectionId, dataSet.FileLocation, dataSet.FileContent}
		addMetaDocument(*collection, metaSet)

	}
}

func addS3Document(collection mgo.Collection, doc S3Set) {
	query := bson.M{"fileLocation": doc.FileLocation}
	change := bson.M{"$set": bson.M{"s3Location": doc.S3Location, "collectionId": doc.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new resource
		insertError := collection.Insert(doc)
		if insertError != nil {
			panic(insertError)
		}
		log.Printf("Collection %q Inserted into %q resource %s", doc.CollectionId, collection.Name, doc.FileLocation)
	} else {
		log.Printf("Collection %q Updated %q with %s", doc.CollectionId, collection.Name, doc.FileLocation)
	}
}

func addMetaDocument(collection mgo.Collection, doc MetaSet) {
	query := bson.M{"fileLocation": doc.FileLocation}
	change := bson.M{"$set": bson.M{"fileContent": doc.FileContent, "collectionId": doc.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new page
		insertError := collection.Insert(doc)
		if insertError != nil {
			panic(insertError)
		}
		log.Printf("Collection %q Inserted page into %s at %s", doc.CollectionId, collection.Name, doc.FileLocation)
	} else {
		log.Printf("Collection %q Updated page in %s at %s", doc.CollectionId, collection.Name, doc.FileLocation)
	}
}

func main() {
	fileCompleteTopic := utils.GetEnvironmentVariable(FILE_COMPLETE_TOPIC_ENV, "uk.gov.ons.dp.web.complete-file")
	fileCompleteConsumer := kafka.NewConsumer(fileCompleteTopic)
	log.Printf("Started publish receiver on %q", fileCompleteTopic)

	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
	if err != nil {
		panic(err)
	}
	defer dbSession.Close()
	db := dbSession.DB(DATA_BASE)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case msg := <-fileCompleteConsumer.Incoming:
			// The db session is concurrency-safe (See https://godoc.org/gopkg.in/mgo.v2#Session)
			go storeData(msg, db)
		case <-signals:
			log.Printf("Service stopped")
			return
		}
	}
}
