package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/health"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/go-ns/log"
	"gopkg.in/olivere/elastic.v5"
)

type Page struct {
	URI         string           `json:"uri"`
	Type        string           `json:"type"`
	Description *PageDescription `json:"description"`
	Topics      []string         `json:"topics"`
}

func (page *Page) isData() bool {
	return strings.Contains(page.Type, "image")
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
	LatestRelease   bool     `json:"latestRelease"`
	DatasetID       string   `json:"datasetId"`
	CDID            string   `json:"cdid"`
	HeadLine1       string   `json:"headline1"`
	HeadLine2       string   `json:"headline2"`
	HeadLine3       string   `json:"headline3"`
	KeyNote         string   `json:"keyNote"`
}

func main() {
	kafkaBrokers := utils.GetEnvironmentVariableAsArray("KAFKA_ADDR", "localhost:9092")
	consumerTopic := utils.GetEnvironmentVariable("KAFKA_CONSUMER_TOPIC", "uk.gov.ons.dp.web.complete-file")
	elasticSearchNodes := utils.GetEnvironmentVariableAsArray("ELASTIC_SEARCH_NODES", "http://127.0.0.1:9200")
	elasticSearchIndex := utils.GetEnvironmentVariable("ELASTIC_SEARCH_INDEX", "ons")
	healthCheckAddr := utils.GetEnvironmentVariable("HEALTHCHECK_ADDR", ":8080")
	healthCheckEndpoint := utils.GetEnvironmentVariable("HEALTHCHECK_ENDPOINT", "/healthcheck")
	log.Namespace = "publish-search-indexer"
	log.Debug("Starting publish search indexer",
		log.Data{"kafka_brokers": kafkaBrokers,
			"kafka_consumer_topic": consumerTopic,
			"elastic_nodes":        elasticSearchNodes,
			"elastic_index":        elasticSearchIndex})

	// Create elastic search client to the HTTP interface
	searchClient, err := elastic.NewClient(
		elastic.SetURL(elasticSearchNodes...),
		elastic.SetMaxRetries(5),
		elastic.SetSniff(false))
	if err != nil {
		log.ErrorC("Failed to create elastic client", err, log.Data{})
		panic(err)
	}

	// To improve indexing performance a BulkProcessor is used to allow multiple web
	// pages to be uploaded and indexed.
	bulk, bulkErr := searchClient.BulkProcessor().
		BulkSize(1000).
		Workers(4).
		FlushInterval(time.Millisecond * 1000).
		After(after).
		Do(context.Background())
	bulk.Start(context.Background())
	if bulkErr != nil {
		log.ErrorC("Failed to create bulk processor", bulkErr, log.Data{})
		panic(bulkErr)
	}

	// Setup kafka using consumer groups
	groupName := "publish-search-index"
	consumer, consumerErr := kafka.NewConsumerGroup(consumerTopic, groupName)
	if err != nil {
		log.ErrorC("Failed to create kafka consumer", consumerErr, log.Data{"topic": consumerTopic,
			"group": groupName})
		panic(consumerErr)
	}

	healthChannel := make(chan bool)
	go func() {
		http.HandleFunc(healthCheckEndpoint, health.NewHealthChecker(healthChannel, nil))
		log.Info(fmt.Sprintf("Listening for %s on %s", healthCheckEndpoint, healthCheckAddr), nil)
		log.ErrorC("healthcheck listener exited", http.ListenAndServe(healthCheckAddr, nil), nil)
		panic("healthcheck listener exited")
	}()

	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			err := processMessage(consumerMessage.GetData(), bulk, elasticSearchIndex)
			if err != nil {
				log.ErrorC("Failed to process kafka message", err, log.Data{})
				panic(err)
			}
			consumerMessage.Commit()
		case err := <-consumer.Errors:
			log.ErrorC("Kafka client error", err, log.Data{})
			panic(err)
		case <-healthChannel:
		}
	}
}

func after(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		log.ErrorC("Failed to upload documents to elastic search", err, log.Data{"response": response})
	} else {
		if response.Errors {
			for _, failedDocument := range response.Failed() {
				log.ErrorC("Document failed to be indexed", fmt.Errorf(failedDocument.Error.Reason),
					log.Data{"type": failedDocument.Type})
			}
		} else {
			log.Debug("Uploaded documents to elastic search", log.Data{"uploaded": len(response.Succeeded())})
		}
	}
}

func processMessage(msg []byte, bulkProcessor *elastic.BulkProcessor, elasticSearchIndex string) error {
	// First deserialise the event to check that its a json file to index.
	var event kafka.FileCompleteMessage
	err := json.Unmarshal(msg, &event)
	if err != nil {
		return err
	}
	if event.S3Location != "" {
		// Skip any messages which contain s3 location as it will not contain any json
		return nil
	}

	// If the message has JSON content, deserialise it as a page.
	var page Page
	err = json.Unmarshal([]byte(event.FileContent), &page)
	// If the page type is nothing it triggers error in elastic search and causes the pipe line
	// to slow down.
	if err != nil || page.Type == "" || page.isData() {
		log.Debug("Page will not be indexed as it does not contain a data type", log.Data{"message": event})
		// nil is returned as no all pages can be indexed.
		return nil
	}

	request := elastic.NewBulkIndexRequest().
		Index(elasticSearchIndex).
		Type(page.Type).
		Id(page.URI).
		Doc(page)

	bulkProcessor.Add(request)
	if err != nil {
		return fmt.Errorf("Failed to add bulk request : %+v", request)
	}
	return nil
}
