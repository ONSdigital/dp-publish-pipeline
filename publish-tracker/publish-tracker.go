package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"

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
		log.Panic(err)
	}
	defer rows.Close()

	var scheduleId sql.NullInt64
	completedTime := time.Now().UnixNano()

	for rows.Next() {
		if err := rows.Scan(&scheduleId); err != nil {
			log.Panic(err)
		}
		duration, collectionId, err := markJobComplete(dbMeta, producer, scheduleId.Int64, completedTime)
		if err != nil {
			log.Printf("WARNING: %s", err)
		} else {
			log.Printf("Job %d Collection %q completes in %s", scheduleId.Int64, collectionId, duration)
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
		log.Panic(err)
	}

	data, _ := json.Marshal(kafka.CollectionCompleteMessage{scheduleId, collectionId.String})
	producer.Output <- data

	return time.Duration(completedTime-startTime.Int64) * time.Nanosecond, collectionId.String, nil
}

func markFileComplete(jsonMessage []byte, dbMeta dbMetaObj) {
	var file kafka.FileCompleteFlagMessage
	if err := json.Unmarshal(jsonMessage, &file); err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if file.ScheduleId == 0 || file.FileId == 0 {
		log.Printf("Json message is missing fields : %s", string(jsonMessage))
		return
	}
	//log.Printf("Updating %d", file.FileId)
	if _, err := dbMeta.prepped["update-completed-file"].Exec(file.FileId, time.Now().UnixNano()); err != nil {
		log.Panicf("Error: Could not update file %d : %s", file.FileId, err)
	}
}

func (dbMeta dbMetaObj) prep(tag, sql string) {
	var err error
	dbMeta.prepped[tag], err = dbMeta.db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare %q statement on database: %s", tag, err.Error())
	}
}

func main() {
	completeFileTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	completeCollectionTopic := utils.GetEnvironmentVariable("COMPLETE_TOPIC", "uk.gov.ons.dp.web.complete")
	maxConcurrentFileCompletes, err := utils.GetEnvironmentVariableInt("MAX_CONCURRENT_FILE_COMPLETES", 40)
	if err != nil {
		log.Fatal("Cannot convert MAX_CONCURRENT_FILE_COMPLETES to integer")
	}
	log.Printf("Starting publish tracker of %q to %q", completeFileTopic, completeCollectionTopic)

	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}
	if err = db.Ping(); err != nil {
		log.Panicf("Error: Could not establish a connection with the database: %s", err.Error())
	}
	dbMeta := dbMetaObj{db: db, prepped: make(map[string]*sql.Stmt)}
	dbMeta.prep("find-completed-jobs", "SELECT schedule_id FROM schedule WHERE complete_time IS NULL AND start_time IS NOT NULL EXCEPT SELECT DISTINCT schedule_id FROM schedule_file WHERE complete_time IS NULL")
	dbMeta.prep("update-completed-file", "UPDATE schedule_file SET complete_time=$2 WHERE schedule_file_id=$1")
	dbMeta.prep("update-complete-job", "UPDATE schedule SET complete_time=$2 WHERE schedule_id=$1 AND start_time IS NOT NULL AND complete_time IS NULL RETURNING collection_id, start_time")

	fileConsumer := kafka.NewConsumerGroup(completeFileTopic, "publish-tracker")
	producer := kafka.NewProducer(completeCollectionTopic)

	rateLimitFileCompletes := make(chan bool, maxConcurrentFileCompletes)
	tickerJobCompleteCheck := time.Tick(tick)

	for {
		select {
		case <-tickerJobCompleteCheck:
			go checkForCompletedJobs(dbMeta, producer)
		case consumerMessage := <-fileConsumer.Incoming:
			rateLimitFileCompletes <- true
			go func() {
				defer func() { <-rateLimitFileCompletes }()
				markFileComplete(consumerMessage.GetData(), dbMeta)
				consumerMessage.Commit()
			}()
		}
	}
}
