package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"

	"database/sql"

	_ "github.com/lib/pq"
)

var (
	tick          = time.Millisecond * 200
	maxConcurrent = 20
	sched         sync.Mutex
)

type scheduleJob struct {
	collectionId  string
	scheduleId    int64
	encryptionKey string
	scheduleTime  int64
	files         []string
}

func findCollectionFiles(zebedeeRoot, collectionId string) ([]string, error) {
	var files []string
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionId, "complete")
	filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("Walk failed: %s", err.Error())
		}
		relPath, relErr := filepath.Rel(searchPath, path)
		if !info.IsDir() && relErr == nil {
			files = append(files, relPath)
		}
		return nil
	})
	return files, nil
}

func publishCollection(message scheduleJob, fileProducerChannel, totalProducerChannel chan []byte) {
	if message.collectionId == "" {
		log.Panicf("Bad message: %v", message)
	}

	var data []byte
	var err error

	if data, err = json.Marshal(kafka.PublishTotalMessage{message.collectionId, len(message.files)}); err != nil {
		log.Panic(err)
	}
	totalProducerChannel <- data

	for i := 0; i < len(message.files); i++ {
		if data, err = json.Marshal(kafka.PublishFileMessage{message.collectionId, message.encryptionKey, message.files[i]}); err != nil {
			log.Panic(err)
		}
		fileProducerChannel <- data
	}
	log.Printf("Collection %q [id %d]- sent %d files", message.collectionId, message.scheduleId, len(message.files))
}

func scheduleCollection(jsonMessage []byte, schedule *[]scheduleJob, zebedeeRoot string, db *sql.DB) {
	var message kafka.ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 {
		log.Panicf("Empty collectionId: %v", jsonMessage)
	} else {
		scheduleTime, err := strconv.ParseInt(message.ScheduleTime, 10, 64)
		if err != nil {
			log.Panicf("Collection %q Cannot numeric convert: %q", message.CollectionId, message.ScheduleTime)
		}
		newJob := scheduleJob{collectionId: message.CollectionId, scheduleTime: scheduleTime, encryptionKey: message.EncryptionKey}

		addIndex := -1

		sched.Lock()
		defer sched.Unlock()

		for i := 0; i < len(*schedule); i++ {
			if (*schedule)[i].collectionId == "" {
				addIndex = i
				break
			}
		}

		var files []string
		if files, err = findCollectionFiles(zebedeeRoot, message.CollectionId); err != nil {
			log.Panic(err)
		}
		//log.Printf("coll %q found %d files", message.CollectionId, len(files))
		newJob.files = make([]string, len(files))
		copy(newJob.files, files)

		newJob.scheduleId = storeJob(db, newJob)

		if addIndex == -1 {
			*schedule = append(*schedule, newJob)
		} else {
			(*schedule)[addIndex] = newJob
		}
		log.Printf("Collection %q [id %d] schedule now len:%d files:%d", message.CollectionId, newJob.scheduleId, len(*schedule), len(newJob.files))
	}
}

func checkSchedule(schedule *[]scheduleJob, publishChannel chan scheduleJob, db *sql.DB) {
	epochTime := time.Now().Unix()
	sched.Lock()
	defer sched.Unlock()
	//log.Printf("%d check len:%d %v", epochTime, len(*schedule), schedule)
	sentJobs := 0
	for i := 0; i < len(*schedule); i++ {
		collectionId := (*schedule)[i].collectionId
		if collectionId == "" {
			continue
		}
		scheduleId := (*schedule)[i].scheduleId
		if (*schedule)[i].scheduleTime <= epochTime {
			log.Printf("Collection %q [id %d] found %d", collectionId, scheduleId, i)
			// message := kafka.ScheduleMessage{CollectionId: collectionId, EncryptionKey: (*schedule)[i].encryptionKey}
			message := (*schedule)[i]
			updateJob(db, message.collectionId, 0, scheduleId)
			(*schedule)[i] = scheduleJob{collectionId: ""}
			publishChannel <- message
			sentJobs++
			if maxConcurrent > 0 && sentJobs > maxConcurrent {
				break
			}
		} else {
			log.Printf("Collection %q [id %d] Not time for %d - %d > %d", collectionId, scheduleId, i, epochTime, (*schedule)[i].scheduleTime)
		}
	}
}

func completeCollection(jsonMessage []byte, schedule *[]scheduleJob, db *sql.DB) {
	var message kafka.CollectionCompleteMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 {
		log.Panicf("Empty collectionId: %v", jsonMessage)
	} else {
		id := updateJob(db, message.CollectionId, time.Now().Unix(), 0)
		log.Printf("Collection %q [id %d] completed", message.CollectionId, id)
	}
}

func loadSchedule(db *sql.DB, schedule *[]scheduleJob, zebedeeRoot string) {
	query, err := db.Prepare("SELECT schedule_id, collection_id, schedule_time, encryption_key FROM schedule WHERE complete_time IS NULL ORDER BY schedule_time")
	if err != nil {
		log.Panicf("DB prepare failed: %s", err.Error())
	}

	rows, err := query.Query()
	defer rows.Close()

	for rows.Next() {
		var collectionId, encryptionKey sql.NullString
		var scheduleTime sql.NullInt64
		var scheduleId sql.NullInt64

		if err = rows.Scan(&scheduleId, &collectionId, &scheduleTime, &encryptionKey); err != nil {
			log.Fatal(err)
		}

		job := scheduleJob{scheduleId: scheduleId.Int64, scheduleTime: scheduleTime.Int64, collectionId: collectionId.String, encryptionKey: encryptionKey.String}
		var files []string
		if files, err = findCollectionFiles(zebedeeRoot, job.collectionId); err != nil {
			log.Panic(err)
		}
		//log.Printf("coll %q found %d files", message.CollectionId, len(files))
		job.files = make([]string, len(files))
		copy(job.files, files)

		*schedule = append(*schedule, job)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d jobs", len(*schedule))
}

func storeJob(db *sql.DB, job scheduleJob) int64 {
	insert, err := db.Prepare("INSERT INTO schedule (collection_id, schedule_time, encryption_key, complete_time) VALUES ($1, $2, $3, NULL) RETURNING schedule_id")
	if err != nil {
		log.Panicf("DB prepare failed: %s", err.Error())
	}
	res := insert.QueryRow(job.collectionId, job.scheduleTime, job.encryptionKey)
	var scheduleId int64
	if err = res.Scan(&scheduleId); err != nil {
		log.Panic(err)
	}
	return scheduleId
}

func updateJob(db *sql.DB, collectionId string, completeTime, scheduleId int64) int64 {
	// if completeTime==0, the job is being started...
	var update *sql.Stmt
	var err error
	dbArgs := make([]interface{}, 2, 3)
	dbArgs[0], dbArgs[1] = collectionId, completeTime
	sql := "UPDATE schedule SET complete_time=$2 WHERE collection_id=$1 AND complete_time=0 RETURNING schedule_id"
	if completeTime == 0 {
		sql = "UPDATE schedule SET complete_time=$2 WHERE schedule_id=$3 AND collection_id=$1 AND complete_time IS NULL RETURNING schedule_id"
		dbArgs = append(dbArgs, scheduleId)
	}
	if update, err = db.Prepare(sql); err != nil {
		log.Panicf("DB prepare %q failed: %s", sql, err.Error())
	}
	res := update.QueryRow(dbArgs...)
	if scheduleId == 0 {
		if err = res.Scan(&scheduleId); err != nil {
			log.Panic(err)
		}
	}
	return scheduleId
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory %q", zebedeeRoot)
	}

	scheduleTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	completeCollectionTopic := utils.GetEnvironmentVariable("COMPLETE_TOPIC", "uk.gov.ons.dp.web.complete")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")

	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}
	if err = db.Ping(); err != nil {
		log.Panicf("Error: Could not establish a connection with the database: %s", err.Error())
	}

	schedule := make([]scheduleJob, 0, 10)

	loadSchedule(db, &schedule, zebedeeRoot)

	log.Printf("Starting publish scheduler from %q topics: %q -> %q/%q", zebedeeRoot, scheduleTopic, produceFileTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	scheduleConsumer := kafka.NewConsumer(scheduleTopic)
	fileProducer := kafka.NewProducer(produceFileTopic)
	completeConsumer := kafka.NewConsumer(completeCollectionTopic)

	publishChannel := make(chan scheduleJob)
	exitChannel := make(chan bool)

	go func() {
		for {
			select {
			case scheduleMessage := <-scheduleConsumer.Incoming:
				go scheduleCollection(scheduleMessage, &schedule, zebedeeRoot, db)
			case publishMessage := <-publishChannel:
				go publishCollection(publishMessage, fileProducer.Output, totalProducer.Output)
			case completeMessage := <-completeConsumer.Incoming:
				go completeCollection(completeMessage, &schedule, db)
			case <-time.After(tick):
				go checkSchedule(&schedule, publishChannel, db)
			}
		}
	}()
	<-exitChannel

	log.Printf("Service publish scheduler stopped")
}
