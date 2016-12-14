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
const MASTER_COLLECTION = "master"
const S3_COLLECTION = "s3"
const S3_PREFIX = ".s3"
const META_PREFIX = ".meta"

const MONGODB_ENV = "MONGODB"
const FILECOMPLETE_TOPIC_ENV = "FILE_COMPLETE_TOPIC"
const COMPLETE_TOPIC_ENV = "COMPLETE_TOPIC"

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
		if dataSet.S3Location != "" {
			collection := db.C(dataSet.CollectionId + S3_PREFIX)
			s3Set := S3Set{dataSet.CollectionId, dataSet.FileLocation, dataSet.S3Location}
			addS3Document(*collection, s3Set)
		} else if dataSet.FileContent != "" {
			collection := db.C(dataSet.CollectionId + META_PREFIX)
			metaSet := MetaSet{dataSet.CollectionId, dataSet.FileLocation, dataSet.FileContent}
			addMetaDocument(*collection, metaSet)
		} else {
			log.Printf("Unknown data from %v", dataSet)
		}
	}
}

func complete(jsonMessage []byte) {
	var release ReleaseCollection
	err := json.Unmarshal(jsonMessage, &release)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else if release.CollectionId != "" {
		session, err := mgo.Dial(utils.GetEnvironmentVariable(MONGODB_ENV, "localhost"))
		if err != nil {
			panic(err)
		}
		defer session.Close()
		db := session.DB(DATA_BASE)
		// Copy S3 content to the public collection
		src := db.C(release.CollectionId + S3_PREFIX)
		copyS3Collection(*src, *db.C(S3_COLLECTION))
		src.DropCollection()

		//Copy meta content to the public collection
		src = db.C(release.CollectionId + META_PREFIX)
		copyMetaCollection(*src, *db.C(MASTER_COLLECTION))
		src.DropCollection()
		log.Printf("Release collectionId : %s", release.CollectionId)
	} else {
		log.Printf("Json mesage missing field %s", string(jsonMessage))
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
		log.Printf("Inserted resource within collection %s at %s", collection.Name, doc.FileLocation)
	} else {
		log.Printf("Updated resource within collection %s at %s", collection.Name, doc.FileLocation)
	}
}

func addMetaDocument(collection mgo.Collection, doc MetaSet) {
	query := bson.M{"fileLocation": doc.FileLocation}
	change := bson.M{"$set": bson.M{"fileContent": doc.FileContent, "collecionId": doc.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new page
		insertError := collection.Insert(doc)
		if insertError != nil {
			panic(insertError)
		}
		log.Printf("Inserted page within collection %s at %s", collection.Name, doc.FileLocation)
	} else {
		log.Printf("Updated page within collection %s at %s", collection.Name, doc.FileLocation)
	}
}

func copyS3Collection(src mgo.Collection, dst mgo.Collection) {
	result := make([]S3Set, 0)
	iter := src.Find(nil).Iter()
	iter.All(&result)
	iter.Close()
	for i := 0; i < len(result); i++ {
		addS3Document(dst, result[i])
	}
}

func copyMetaCollection(src mgo.Collection, dst mgo.Collection) {
	result := make([]MetaSet, 0)
	iter := src.Find(nil).Iter()
	iter.All(&result)
	iter.Close()
	for i := 0; i < len(result); i++ {
		addMetaDocument(dst, result[i])
	}
}

func main() {
	fileCompleteTopic := utils.GetEnvironmentVariable(FILECOMPLETE_TOPIC_ENV, "uk.gov.ons.dp.web.complete-file")
	completeTopic := utils.GetEnvironmentVariable(COMPLETE_TOPIC_ENV, "uk.gov.ons.dp.web.complete")
	master := kafka.CreateConsumer()
	log.Printf("Started publish receiver")
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	fileCompleteChannel := make(chan []byte, 10)
	completeChannel := make(chan []byte, 10)
	kafka.ProcessMessages(master, fileCompleteTopic, fileCompleteChannel)
	kafka.ProcessMessages(master, completeTopic, completeChannel)

	for {
		select {
		case msg := <-fileCompleteChannel:
			storeData(msg)
		case msg := <-completeChannel:
			complete(msg)
		case <-signals:
			log.Printf("Service stopped")
			return
		}
	}
}
