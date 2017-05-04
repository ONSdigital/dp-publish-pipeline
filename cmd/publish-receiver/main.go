package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	_ "github.com/lib/pq"
)

const FILE_COMPLETE_TOPIC_ENV = "FILE_COMPLETE_TOPIC"

func storeData(jsonMessage []byte, s3 *sql.Stmt, meta *sql.Stmt) {
	var dataSet kafka.FileCompleteMessage
	err := json.Unmarshal(jsonMessage, &dataSet)
	if err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if dataSet.CollectionId == "" || dataSet.Uri == "" {
		log.Printf("Unknown data from %v", dataSet)
		return
	}
	if dataSet.S3Location != "" {
		addS3Data(dataSet, s3)
	} else if dataSet.FileContent != "" {
		addMetadata(dataSet, meta)
	}
}

func addS3Data(dataSet kafka.FileCompleteMessage, s3 *sql.Stmt) {
	results, err := s3.Query(dataSet.CollectionId,
		resolveURI(dataSet.Uri),
		dataSet.S3Location)
	if err != nil {
		log.Printf("Error : %s", err.Error())
	} else {
		log.Printf("Job %d Collection %q Added S3 : %s", dataSet.ScheduleId, dataSet.CollectionId, dataSet.Uri)
		results.Close()
	}
}

func addMetadata(dataSet kafka.FileCompleteMessage, meta *sql.Stmt) {
	lang := getLanguage(dataSet.Uri)
	results, err := meta.Query(dataSet.CollectionId,
		resolveURI(dataSet.Uri)+"?lang="+lang,
		dataSet.FileContent)
	if err != nil {
		log.Printf("Error : %s", err.Error())
	} else {
		log.Printf("Job %d Collection %q Added metadata : %s", dataSet.ScheduleId, dataSet.CollectionId, dataSet.Uri)
		results.Close()
	}
}

func getLanguage(uri string) string {
	if strings.HasSuffix(uri, "data_cy.json") {
		return "cy"
	}
	return "en"
}

// Within the zebedee reader it builds the uri based of what it is given. Instead
// of repeating this per HTTP request, postgrese stores the URI the website expects
// So the content-api does not need to build the uri each time.
// Examples :
//  File location                : URI
//  data.json                    => / (Special case for root file)
//  about/data.json              => /about
//  timeseries/mmg/hhh/data.json => /timeseries/mmg/hhh
//  trade/report/938438.json     => /trade/report/938438 (Special case for charts)
func resolveURI(uri string) string {
	if strings.HasSuffix(uri, "/data.json") || strings.HasSuffix(uri, "/data_cy.json") {
		webURI := filepath.Dir(uri)
		return webURI
	} else if strings.HasSuffix(uri, ".json") {
		return uri[:len(uri)-5]
	}
	return uri
}

func prep(sql string, db *sql.DB) *sql.Stmt {
	statement, err := db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	return statement
}

func main() {
	fileCompleteTopic := utils.GetEnvironmentVariable(FILE_COMPLETE_TOPIC_ENV, "uk.gov.ons.dp.web.complete-file")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")

	fileCompleteConsumer, err := kafka.NewConsumerGroup(fileCompleteTopic, "publish-receiver")
	if err != nil {
		log.Panicf("Could not obtain consumer: %s", err)
	}

	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}

	s3Upsert := "INSERT INTO s3data(collection_id, uri, s3) VALUES($1, $2, $3) " +
		"ON CONFLICT(uri) DO UPDATE " +
		"SET (collection_id, s3) = ($1, $3)"
	s3statement := prep(s3Upsert, db)
	defer s3statement.Close()

	metaUpsert := "INSERT INTO metadata(collection_id, uri, content) VALUES($1, $2, $3) " +
		"ON CONFLICT(uri) DO UPDATE " +
		"SET (collection_id, content) = ($1, $3)"
	metaStatement := prep(metaUpsert, db)
	defer metaStatement.Close()

	log.Printf("Started publish receiver on %q", fileCompleteTopic)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case consumerMessage := <-fileCompleteConsumer.Incoming:
			storeData(consumerMessage.GetData(), s3statement, metaStatement)
			consumerMessage.Commit()
		case errorMessage := <-fileCompleteConsumer.Errors:
			log.Panicf("Aborting: %s", errorMessage)
		case <-signals:
			log.Printf("Service stopped")
			return
		}
	}
}
