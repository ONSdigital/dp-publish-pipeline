package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/bsm/sarama-cluster"
	"gopkg.in/olivere/elastic.v3"
)

type FileCompleteEvent struct {
	FileContent  string `json:"fileContent"`
	FileLocation string `json:"fileLocation"`
	CollectionId string `json:"collectionId"`
}

type Page struct {
	URI         string           `json:"uri"`
	Type        string           `json:"type"`
	Description *PageDescription `json:"description"`
}

type PageDescription struct {
	Title           string   `json:"title"`
	Summary         string   `json:"summary"`
	MetaDescription string   `json:"metaDescription"`
	Keywords        []string `json:"keywords"`
	Unit            string   `json:"unit"`
	PreUnit         string   `json:"preUnit"`
	Source          string   `json:"source"`
	ReleaseDate     string   `json:"releaseDate"`
}

func main() {

	kafkaBrokers := []string{utils.GetEnvironmentVariable("KAFKA_ADDR", "localhost:9092")}
	kafkaConsumerTopic := utils.GetEnvironmentVariable("KAFKA_CONSUMER_TOPIC", "uk.gov.ons.dp.web.complete-file")
	kafkaConsumerGroup := utils.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP", "uk.gov.ons.dp.web.complete-file.search-index")
	elasticSearchNodes := []string{utils.GetEnvironmentVariable("ELASTIC_SEARCH_NODES", "http://127.0.0.1:9200")}
	elasticSearchIndex := utils.GetEnvironmentVariable("ELASTIC_SEARCH_INDEX", "ons")

	log.Print("Environment variable values:")
	log.Printf(" - KAFKA_ADDR %v", kafkaBrokers)
	log.Printf(" - KAFKA_CONSUMER_TOPIC %v", kafkaConsumerTopic)
	log.Printf(" - KAFKA_CONSUMER_GROUP %v", kafkaConsumerGroup)
	log.Printf(" - ELASTIC_SEARCH_NODES %v", elasticSearchNodes)
	log.Printf(" - ELASTIC_SEARCH_INDEX %v", elasticSearchIndex)

	log.Print("Creating elastic search client.")
	searchClient, err := elastic.NewClient(
		elastic.SetURL(elasticSearchNodes...),
		elastic.SetMaxRetries(5))
	if err != nil {
		log.Fatalf("An error occured creating the Elastic Search client: %+v", err)
		return
	}
	log.Print("Elastic Search client Created successfully.")

	bulk, _ := searchClient.BulkProcessor().
		BulkSize(1000).
		Workers(4).
		FlushInterval(time.Millisecond * 1000).
		After(after).
		Do()

	bulk.Start()
	log.Printf("Creating Kafka consumer.")
	consumerConfig := cluster.NewConfig()
	kafkaConsumer, err := cluster.NewConsumer(kafkaBrokers, kafkaConsumerGroup, []string{kafkaConsumerTopic}, consumerConfig)
	if err != nil {
		log.Fatalf("An error occured creating the Kafka consumer: %+v", err)
		return
	}
	log.Printf("Kafka consumer created.")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case msg := <-kafkaConsumer.Messages():
			processMessage(msg.Value, bulk, elasticSearchIndex)
		case <-signals:
			log.Print("Shutting down...")
			bulk.Stop()
			kafkaConsumer.Close()
			searchClient.Stop()
			log.Printf("Service stopped")
			return
		}
	}
}

func after(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		log.Printf("Failed to bulk upload documents : %s", err.Error())
		log.Printf("Number of documents failed : %d", len(response.Failed()))
	} else {
		log.Printf("Uploaded %d documents to the ONS index.", len(response.Succeeded()))
	}
}

func processMessage(msg []byte, elasticSearchClient *elastic.BulkProcessor, elasticSearchIndex string) {

	// First deserialise the event to check that its a json file to index.
	var event FileCompleteEvent
	err := json.Unmarshal(msg, &event)
	if err != nil {
		log.Printf("Failed to parse json event data")
		return
	}
	if event.FileContent == "" {
		log.Printf("Ignoring %s in collection, it has no JSON content.", event.FileLocation)
		return
	}

	// If the message has JSON content, deserialise it as a page.
	var page Page
	err = json.Unmarshal([]byte(event.FileContent), &page)
	// If the page type is nothing it triggers error in elastic search and causes the pipe line
	// to slow down.
	if err != nil || page.Type == "" {
		log.Printf("Failed to parse json page data: %+v", event)
		return
	}

	r := elastic.NewBulkIndexRequest().
		Index(elasticSearchIndex).
		Type(page.Type).
		Id(page.URI).
		Doc(page)

	elasticSearchClient.Add(r)
	if err != nil {
		log.Print(err)
	}
}
