package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	mongo "github.com/ONSdigital/dp-publish-pipeline/mongodb"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

const FILE_COMPLETE_TOPIC_ENV = "FILE_COMPLETE_TOPIC"

func storeData(jsonMessage []byte, client *mongo.MongoClient) {
	var dataSet kafka.FileCompleteMessage
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
		s3document := mongo.S3Document{dataSet.CollectionId, dataSet.FileLocation, dataSet.S3Location}
		addS3Document(client, s3document)
	} else if dataSet.FileContent != "" {
		metaDocument := mongo.MetaDocument{dataSet.CollectionId, dataSet.FileLocation, dataSet.FileContent}
		addMetaDocument(client, metaDocument)
	}
}

func addS3Document(client *mongo.MongoClient, doc mongo.S3Document) {
	err := client.AddS3Data(doc)
	if err != nil {
		log.Fatalf("Failed to add s3 document. S3Document %+v :", doc)
	}
	log.Printf("Collection %q Inserted into %q resource %s", doc.CollectionId, "S3", doc.FileLocation)
}

func addMetaDocument(client *mongo.MongoClient, doc mongo.MetaDocument) {
	doc.FileLocation = resloveURI(doc.FileLocation)
	err := client.AddPage(doc)
	if err != nil {
		log.Fatalf("Failed to add meta document. MetaDocument %+v :", doc)
	}
	log.Printf("Collection %q Inserted page into %s at %s", doc.CollectionId, "meta", doc.FileLocation)
}

// Within the zebedee reader it builds the uri based of what it is given. Instead
// of repeating this per HTTP request, mongo stores the URI the website expects
// So the content-api does not need to build the uri each time.
// Examples :
//  File location                : URI
//  data.json                    => / (Special case for root file)
//  about/data.json              => /about
//  timeseries/mmg/hhh/data.json => /timeseries/mmg/hhh
//  trade/report/938438.json     => /trade/report/938438 (Special case for charts)
func resloveURI(uri string) string {
	if strings.Contains(uri, "data.json") {
		if uri == "data.json" {
			return "/"
		}
		webURI := "/" + uri[:len(uri)-10]
		return webURI
	} else if strings.Contains(uri, ".json") {
		return "/" + uri[:len(uri)-5]
	}
	return uri
}

func main() {
	fileCompleteTopic := utils.GetEnvironmentVariable(FILE_COMPLETE_TOPIC_ENV, "uk.gov.ons.dp.web.complete-file")
	fileCompleteConsumer := kafka.NewConsumerGroup(fileCompleteTopic, "publish-receiver")

	client, connectionErr := mongo.CreateClient()
	defer client.Close()
	if connectionErr != nil {
		log.Fatalf("Failed to connect to mongodb. Error : %s", connectionErr.Error())
	}

	log.Printf("Started publish receiver on %q", fileCompleteTopic)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case consumerMessage := <-fileCompleteConsumer.Incoming:
			storeData(consumerMessage.GetData(), &client)
			consumerMessage.Commit()
		case <-signals:
			log.Printf("Service stopped")
			return
		}
	}
}
