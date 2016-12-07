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
	FileLocation string
	FileContent  string
	CollectionId string
}

func storeData(jsonMessage []byte) {
	var dataSet DataSet
	err := json.Unmarshal(jsonMessage, &dataSet)
	if err != nil {
		log.Printf("Failed to parse json message")
	} else {
		session, err := mgo.Dial(utils.GetEnvironmentVariable("MONGODB", "localhost"))
		if err != nil {
			panic(err)
		}
		defer session.Close()
		collection := session.DB("ONSWebsite").C("master")
		// Try to update the existing document. The Mongo driver sets keys to be
		// all lower case.
		query := bson.M{"filelocation": dataSet.FileLocation}
		change := bson.M{"$set": bson.M{"filecontent": dataSet.FileContent, "collecionid": dataSet.CollectionId}}
		updateErr := collection.Update(query, change)
		if updateErr != nil {
			// No document existed so this must be a new page
			insertError := collection.Insert(dataSet)
			if insertError != nil {
				panic(insertError)
			}
			log.Printf("Inserted page at %s", dataSet.FileLocation)
		} else {
			log.Printf("Updated page at %s", dataSet.FileLocation)
		}
	}
}

func main() {
	log.Printf("Starting publish receiver")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	topic := utils.GetEnvironmentVariable("TOPIC", "test")
	kafka.ProcessMessages(master, topic, storeData)
	log.Printf("Service stopped")
}
