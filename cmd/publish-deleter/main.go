package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/go-ns/log"
	_ "github.com/lib/pq"
)

func publishDelete(jsonMessage []byte, deleteStatement *sql.Stmt, elasticClient *elastic.Client, producer chan []byte) error {
	var message kafka.PublishDeleteMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		return err
	}
	log.Trace("delete", log.Data{"msg": message})
	if message.CollectionId == "" || message.Uri == "" {
		return errors.New("Missing json parameters")
	}

	log.Trace(fmt.Sprintf("Deleting content at %q from postgres from collection %q", message.Uri, message.CollectionId), nil)
	_, sqlErr := deleteStatement.Exec(message.Uri + "?lang=%")
	if sqlErr != nil {
		return sqlErr
	}

	elasticClient.DeleteByQuery("/ons/_all/_query?q=id:" + message.Uri).Do(context.Background())
	producer <- jsonMessage
	return nil
}

func createPostgresConnection() (*sql.DB, error) {
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	db, err := sql.Open("postgres", dbSource)
	return db, err
}

func prepareSQLDeleteStatement(db *sql.DB) *sql.Stmt {
	deleteContentSQL := "DELETE FROM metadata WHERE uri LIKE $1"
	statement, err := db.Prepare(deleteContentSQL)
	if err != nil {
		log.ErrorC("Could not prepare statement on database", err, nil)
		panic(err)
	}
	return statement
}

func createElasticSearchClient() (*elastic.Client, error) {
	elasticSearchNodes := []string{utils.GetEnvironmentVariable("ELASTIC_SEARCH_NODES", "http://127.0.0.1:9200")}
	searchClient, err := elastic.NewClient(
		elastic.SetURL(elasticSearchNodes...),
		elastic.SetMaxRetries(5),
		elastic.SetSniff(false))
	return searchClient, err
}

func main() {
	log.Namespace = "publish-deleter"

	consumerTopic := utils.GetEnvironmentVariable("DELETE_TOPIC", "uk.gov.ons.dp.web.publish-delete")
	producerTopic := utils.GetEnvironmentVariable("PUBLISH_DELETE_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	db, err := createPostgresConnection()
	if err != nil {
		log.Error(err, nil)
		panic(err)
	}
	defer db.Close()
	deleteStatement := prepareSQLDeleteStatement(db)
	defer deleteStatement.Close()

	elasticClient, err := createElasticSearchClient()
	if err != nil {
		log.ErrorC("error creating the Elastic Search client", err, nil)
		panic(err)
	}

	log.Info("Starting Publish-deletes", nil)
	consumer, err := kafka.NewConsumerGroup(consumerTopic, "publish-deletes")
	if err != nil {
		log.Error(err, nil)
		panic(err)
	}
	producer := kafka.NewProducer(producerTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			if err := publishDelete(consumerMessage.GetData(), deleteStatement, elasticClient, producer.Output); err != nil {
				log.Error(err, nil)
				panic(err)
			} else {
				consumerMessage.Commit()
			}
		case errorMessage := <-consumer.Errors:
			log.Error(fmt.Errorf("Aborting: %+v", errorMessage), nil)
			panic("got consumer error")
		}
	}
}
