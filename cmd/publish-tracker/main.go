package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/health"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"

	_ "github.com/lib/pq"
)

var (
	tick = time.Millisecond * 260
)

type dbMetaObj struct {
	db      *sql.DB
	prepped map[string]*sql.Stmt
}

// have all files been completed for jobs yet to be marked as complete
func checkForCompletedJobs(dbMeta dbMetaObj, producer kafka.Producer) {
	rows, err := dbMeta.prepped["find-completed-jobs"].Query()
	if err != nil {
		log.Error(err, nil)
		panic(err)
	}
	defer rows.Close()

	var scheduleId, deletesRemaining, filesRemaining sql.NullInt64
	completedTime := time.Now().UnixNano()

	for rows.Next() {
		if err := rows.Scan(&scheduleId, &deletesRemaining, &filesRemaining); err != nil {
			log.Error(err, nil)
			panic(err)
		}
		if deletesRemaining.Int64 == 0 && filesRemaining.Int64 == 0 {
			duration, collectionId, err := markJobComplete(dbMeta, producer, scheduleId.Int64, completedTime)
			if err != nil {
				log.Error(err, nil)
			} else {
				log.Info(fmt.Sprintf("Job %d Collection %q completes in %s", scheduleId.Int64, collectionId, duration), nil)
			}
		}
	}
}

func markJobComplete(dbMeta dbMetaObj, producer kafka.Producer, scheduleId, completedTime int64) (time.Duration, string, error) {
	var startTime sql.NullInt64
	var collectionId sql.NullString
	res := dbMeta.prepped["update-complete-job"].QueryRow(scheduleId, completedTime)
	if err := res.Scan(&collectionId, &startTime); err != nil {
		if err == sql.ErrNoRows {
			return 0, "", fmt.Errorf("Job %d already complete?", scheduleId)
		}
		log.Error(err, nil)
		panic(err)
	}

	data, _ := json.Marshal(kafka.CollectionCompleteMessage{scheduleId, collectionId.String})
	producer.Output <- data

	return time.Duration(completedTime-startTime.Int64) * time.Nanosecond, collectionId.String, nil
}

func markFileComplete(jsonMessage []byte, dbMeta dbMetaObj) {
	var file kafka.FileCompleteFlagMessage
	if err := json.Unmarshal(jsonMessage, &file); err != nil {
		log.ErrorC("Failed to parse json message", err, log.Data{"json": jsonMessage})
		return
	}
	if file.ScheduleId == 0 || (file.FileId == 0 && file.DeleteId == 0) {
		log.Error(errors.New("Json message is missing fields"), log.Data{"json": string(jsonMessage)})
		return
	}

	if file.FileId != 0 {
		if _, err := dbMeta.prepped["update-completed-file"].Exec(file.FileId, time.Now().UnixNano()); err != nil {
			log.ErrorC("Could not update file", err, log.Data{"fileId": file.FileId})
			panic(err)
		}
	} else if file.DeleteId != 0 {
		if _, err := dbMeta.prepped["update-delete-file"].Exec(file.DeleteId, time.Now().UnixNano()); err != nil {
			log.ErrorC("Could not update delete", err, log.Data{"deleteId": file.DeleteId})
			panic(err)
		}
	}
}

func (dbMeta dbMetaObj) prep(tag, sql string) {
	var err error
	dbMeta.prepped[tag], err = dbMeta.db.Prepare(sql)
	if err != nil {
		log.ErrorC("Could not prepare statement on database", err, log.Data{"tag": tag})
		panic(err)
	}
}

func main() {
	log.Namespace = "publish-tracker"
	completeFileTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	completeCollectionTopic := utils.GetEnvironmentVariable("COMPLETE_TOPIC", "uk.gov.ons.dp.web.complete")
	healthCheckAddr := utils.GetEnvironmentVariable("HEALTHCHECK_ADDR", ":8080")
	healthCheckEndpoint := utils.GetEnvironmentVariable("HEALTHCHECK_ENDPOINT", "/healthcheck")
	maxConcurrentFileCompletes, err := utils.GetEnvironmentVariableInt("MAX_CONCURRENT_FILE_COMPLETES", 40)
	if err != nil {
		log.ErrorC("Cannot convert MAX_CONCURRENT_FILE_COMPLETES to integer", err, nil)
		panic(err)
	}
	log.Info(fmt.Sprintf("Starting publish tracker of %q to %q", completeFileTopic, completeCollectionTopic), nil)

	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.ErrorC("DB open error", err, nil)
		panic(err)
	}
	if err = db.Ping(); err != nil {
		log.ErrorC("Could not establish a connection with the database", err, nil)
		panic(err)
	}
	dbMeta := dbMetaObj{db: db, prepped: make(map[string]*sql.Stmt)}
	dbMeta.prep("find-completed-jobs", "SELECT schedule.schedule_id, (SELECT count(*) FROM schedule_delete WHERE schedule.schedule_id = schedule_delete.schedule_id AND schedule_delete.complete_time IS NULL) AS deletes_remaining, (SELECT count(*) FROM schedule_file WHERE schedule.schedule_id = schedule_file.schedule_id AND schedule_file.complete_time IS NULL) AS files_remaining FROM schedule WHERE complete_time is NULL GROUP BY schedule.schedule_id")
	dbMeta.prep("update-completed-file", "UPDATE schedule_file SET complete_time=$2 WHERE schedule_file_id=$1")
	dbMeta.prep("update-delete-file", "UPDATE schedule_delete SET complete_time=$2 WHERE schedule_delete_id=$1")
	dbMeta.prep("update-complete-job", "UPDATE schedule SET complete_time=$2 WHERE schedule_id=$1 AND start_time IS NOT NULL AND complete_time IS NULL RETURNING collection_id, start_time")
	dbMeta.prep("healthcheck", "SELECT 1 FROM schedule_delete")

	fileConsumer, err := kafka.NewConsumerGroup(completeFileTopic, "publish-tracker")
	if err != nil {
		log.ErrorC("Could not obtain consumer", err, nil)
		panic(err)
	}
	producer := kafka.NewProducer(completeCollectionTopic)

	rateLimitFileCompletes := make(chan bool, maxConcurrentFileCompletes)
	healthChannel := make(chan bool)

	go func() {
		tock := time.Tick(tick)
		for _ = range tock {
			checkForCompletedJobs(dbMeta, producer)
		}
	}()

	go func() {
		http.HandleFunc(healthCheckEndpoint, health.NewHealthChecker(healthChannel, dbMeta.prepped["healthcheck"]))
		log.Info(fmt.Sprintf("Listening for %s on %s", healthCheckEndpoint, healthCheckAddr), nil)
		log.ErrorC("healthcheck listener exited", http.ListenAndServe(healthCheckAddr, nil), nil)
		panic("healthcheck listener exited")
	}()

	for {
		select {
		case consumerMessage := <-fileConsumer.Incoming:
			rateLimitFileCompletes <- true
			go func() {
				defer func() { <-rateLimitFileCompletes }()
				markFileComplete(consumerMessage.GetData(), dbMeta)
				consumerMessage.Commit()
			}()
		case errorMessage := <-fileConsumer.Errors:
			log.Error(errors.New("Aborting after consumer error"), log.Data{"msg": errorMessage})
			panic("Aborting after consumer error")
		case <-healthChannel:
		}
	}
}
