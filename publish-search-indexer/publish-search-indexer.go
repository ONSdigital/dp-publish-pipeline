package main

import (
	"encoding/json"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/bsm/sarama-cluster"
	"gopkg.in/olivere/elastic.v3"
	"log"
	"os"
	"os/signal"
)

type FileCompleteEvent struct {
	FileContent  string `json:"fileContent"`
	FileLocation string `json:"fileLocation"`
	CollectionId string `json:"collectionId"`
}

type Page struct {
	URI         string
	Type        string
	Description *PageDescription
}

type PageDescription struct {
	Title           string
	Summary         string
	MetaDescription string
	Keywords        []string
	Unit            string
	PreUnit         string
	Source          string
	ReleaseDate     string
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
			log.Printf("message received: %+v\n", msg)
			processMessage(msg.Value, searchClient, elasticSearchIndex)
		case <-signals:
			log.Print("Shutting down...")
			kafkaConsumer.Close()
			searchClient.Stop()
			log.Printf("Service stopped")
			return
		}
	}
}

func processMessage(msg []byte, elasticSearchClient *elastic.Client, elasticSearchIndex string) {

	// First deserialise the event to check that its a json file to index.
	var event FileCompleteEvent
	err := json.Unmarshal(msg, &event)
	if err != nil {
		log.Printf("Failed to parse json event data: %s", msg)
		return
	}
	if event.FileContent == "" {
		log.Printf("Ignoring %v in collection, it has no JSON content.", event.FileLocation, event.CollectionId)
		return
	}

	// If the message has JSON content, deserialise it as a page.
	var page Page
	err = json.Unmarshal([]byte(event.FileContent), &page)
	if err != nil {
		log.Printf("Failed to parse json page data: %s", msg)
		return
	}

	log.Printf("Updating search index for uri:%v", page.URI)
	log.Printf("%+v", page)
	_, err = elasticSearchClient.Index().
		Index(elasticSearchIndex).
		Type(page.Type).
		Id(page.URI).
		BodyJson(page).
		Refresh(true).
		Do()
	if err != nil {
		log.Print(err)
	}
}