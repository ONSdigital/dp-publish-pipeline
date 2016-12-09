package main

import (
	"encoding/json"
	"log"

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
const MASTER_COLLECTION = "master"
const S3_COLLECTION = "s3"

const MONGODB_ENV = "MONGODB"
const CONSUMER_TOPIC_ENV = "CONSUME_TOPIC"

func storeData(jsonMessage []byte) {
	var dataSet DataSet
	err := json.Unmarshal(jsonMessage, &dataSet)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else if dataSet.CollectionId != "" && dataSet.FileLocation != "" {
		session, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
		if err != nil {
			panic(err)
		}
		defer session.Close()
		db := session.DB(DATA_BASE)
		if dataSet.FileContent != "" {
			collection := db.C(MASTER_COLLECTION)
			metaContent(*collection, dataSet)
		} else if dataSet.S3Location != "" {
			collection := db.C(S3_COLLECTION)
			s3Content(*collection, dataSet)
		} else {
			log.Printf("Unknown data from %v", dataSet)
		}
	}
}

func s3Content(collection mgo.Collection, dataSet DataSet) {
	query := bson.M{"fileLocation": dataSet.FileLocation}
	change := bson.M{"$set": bson.M{"s3Location": dataSet.S3Location, "collecionId": dataSet.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new resource
		s3Set := S3Set{dataSet.CollectionId, dataSet.FileLocation, dataSet.S3Location}
		insertError := collection.Insert(s3Set)
		if insertError != nil {
			panic(insertError)
		}
		log.Printf("Inserted resource at %s", dataSet.FileLocation)
	} else {
		log.Printf("Updated resource at %s", dataSet.FileLocation)
	}
}

func metaContent(collection mgo.Collection, dataSet DataSet) {
	query := bson.M{"fileLocation": dataSet.FileLocation}
	change := bson.M{"$set": bson.M{"fileContent": dataSet.FileContent, "collecionId": dataSet.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new page
		metaSet := MetaSet{dataSet.CollectionId, dataSet.FileLocation, dataSet.FileContent}
		insertError := collection.Insert(metaSet)
		if insertError != nil {
			panic(insertError)
		}
		log.Printf("Inserted page at %s", dataSet.FileLocation)
	} else {
		log.Printf("Updated page at %s", dataSet.FileLocation)
	}
}

func main() {
	topic := utils.GetEnvironmentVariable(CONSUMER_TOPIC_ENV, "uk.gov.ons.dp.web.complete-file")
	master := kafka.CreateConsumer()
	log.Printf("Started publish receiver on topic '%s'", topic)
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	kafka.ProcessMessages(master, topic, storeData)
	log.Printf("Service stopped")
}
