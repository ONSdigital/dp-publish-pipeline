package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	_ "github.com/lib/pq"
)

func publishDelete(jsonMessage []byte, deleteStatement *sql.Stmt) error {
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
	_, sqlErr := deleteStatement.Query(message.DeleteLocation + "?lang=en")
	if sqlErr != nil {
		return sqlErr
	}
	return nil
}

func createPostgresConnection() (*sql.DB, error) {
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	db, err := sql.Open("postgres", dbSource)
	return db, err
}

func prepareSQLDeleteStatement(db *sql.DB) *sql.Stmt {
	deleteContentSQL := "DELETE from metadata where uri = $1"
	statement, err := db.Prepare(deleteContentSQL)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	return statement
}

func main() {
	consumerTopic := utils.GetEnvironmentVariable("CONSUME_TOPIC", "uk.gov.ons.dp.web.delete-file")
	db, err := createPostgresConnection()
	if err != nil {
		log.Panicf("Database error : %s", err.Error())
	}
	defer db.Close()
	deleteStatement := prepareSQLDeleteStatement(db)
	defer deleteStatement.Close()
	log.Printf("Starting Publish-deletes")
	consumer := kafka.NewConsumerGroup(consumerTopic, "publish-deletes")
	for {
		select {
		case consumerMessage := <-consumer.Incoming:
			if err := publishDelete(consumerMessage.GetData(), deleteStatement); err != nil {
				log.Print(err)
			} else {
				consumerMessage.Commit()
			}
		}
	}
}
