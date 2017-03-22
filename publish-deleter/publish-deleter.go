package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"

	elastic "gopkg.in/olivere/elastic.v3"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	_ "github.com/lib/pq"
)

func publishDelete(jsonMessage []byte, deleteStatement *sql.Stmt, elasticClient *elastic.Client, producer chan []byte) error {
	var message kafka.PublishDeleteMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		return err
	}
	log.Printf("%+v", message)
	if message.CollectionId == "" || message.DeleteLocation == "" {
		return errors.New("Missing json parameters")
	}

	log.Printf("Deleting content at %q from postgres from collection %q", message.DeleteLocation, message.CollectionId)
	_, sqlErr := deleteStatement.Exec(message.DeleteLocation + "?lang=%")
	if sqlErr != nil {
		return sqlErr
	}

	elasticClient.DeleteByQuery("/ons/_all/_query?q=id:" + message.DeleteLocation).Do()
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
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
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
	consumerTopic := utils.GetEnvironmentVariable("DELETE_TOPIC", "uk.gov.ons.dp.web.publish-delete")
	producerTopic := utils.GetEnvironmentVariable("PUBLISH_DELETE_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	db, err := createPostgresConnection()
	if err != nil {
		log.Panicf("Database error : %s", err.Error())
	}
	defer db.Close()
	deleteStatement := prepareSQLDeleteStatement(db)
	defer deleteStatement.Close()

	elasticClient, err := createElasticSearchClient()
	if err != nil {
		log.Fatalf("An error occured creating the Elastic Search client: %s", err.Error())
		return
	}

	log.Printf("Starting Publish-deletes")
	consumer := kafka.NewConsumerGroup(consumerTopic, "publish-deletes")
	producer := kafka.NewProducer(producerTopic)
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			if err := publishDelete(consumerMessage.GetData(), deleteStatement, elasticClient, producer.Output); err != nil {
				log.Print(err)
			} else {
				consumerMessage.Commit()
			}
		}
	}
}
