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
	tick             = time.Millisecond * 330
	inFlight         = 0
	maxInFlight      = 40
	maxLaunchPerTick = 20
	sched            sync.Mutex
)

type dbMetaObj struct {
	db      *sql.DB
	prepped map[string]*sql.Stmt
}

type scheduleJob struct {
	collectionId  string
	scheduleId    int64
	encryptionKey string
	scheduleTime  int64
	files         []string
	filesToDo     int
	isInFlight    bool
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

func publishCollection(job scheduleJob, fileProducerChannel, totalProducerChannel chan []byte) {
	if job.collectionId == "" {
		log.Panicf("Bad message: %v", job)
	}

	var data []byte
	var err error

	if data, err = json.Marshal(kafka.PublishTotalMessage{job.collectionId, job.filesToDo}); err != nil {
		log.Panic(err)
	}
	totalProducerChannel <- data

	for i := 0; i < job.filesToDo; i++ {
		if data, err = json.Marshal(kafka.PublishFileMessage{job.collectionId, job.encryptionKey, job.files[i]}); err != nil {
			log.Panic(err)
		}
		fileProducerChannel <- data
	}
	log.Printf("Collection %q [id %d] sent %d files", job.collectionId, job.scheduleId, job.filesToDo)
}

func scheduleCollection(jsonMessage []byte, schedule *[]scheduleJob, zebedeeRoot string, dbMeta dbMetaObj) {
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
		scheduleTime *= 1000 * 1000 * 1000 // convert from epoch (seconds) to epoch-nanoseconds (UnixNano)
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
		newJob.filesToDo = len(files)
		newJob.files = make([]string, newJob.filesToDo)
		copy(newJob.files, files)

		newJob.scheduleId = storeJob(dbMeta, newJob)

		if addIndex == -1 {
			*schedule = append(*schedule, newJob)
			addIndex = len(*schedule) - 1
		} else {
			(*schedule)[addIndex] = newJob
		}
		log.Printf("Collection %q [id %d] schedule new idx %d now len:%d files:%d", newJob.collectionId, newJob.scheduleId, addIndex, len(*schedule), newJob.filesToDo)
	}
}

func checkSchedule(schedule *[]scheduleJob, publishChannel chan scheduleJob, dbMeta dbMetaObj) {
	epochTime := time.Now().UnixNano()
	launchedThisTick := 0

	sched.Lock()
	defer sched.Unlock()

	for i := 0; i < len(*schedule); i++ {
		job := (*schedule)[i]
		collectionId := job.collectionId
		if collectionId == "" || job.isInFlight {
			continue
		}
		scheduleId := job.scheduleId
		if job.scheduleTime <= epochTime {
			if (maxLaunchPerTick > 0 && launchedThisTick >= maxLaunchPerTick) || (maxInFlight > 0 && inFlight >= maxInFlight) {
				log.Printf("Collection %q [id %d] skip busy idx[%d]- this-tick: %d/%d in-flight: %d/%d", collectionId, scheduleId, i, launchedThisTick, maxLaunchPerTick, inFlight, maxInFlight)
				continue
			}
			startTime := time.Now().UnixNano()
			log.Printf("Collection %q [id %d] found idx[%d] + in-flight: %d - start: %d", collectionId, scheduleId, i, inFlight, startTime)
			updateJobAsStarted(dbMeta, scheduleId, collectionId, startTime)
			(*schedule)[i].isInFlight = true
			publishChannel <- job
			launchedThisTick++
			inFlight++
		} else {
			log.Printf("Collection %q [id %d] Not time for idx %d - %d > %d", collectionId, scheduleId, i, epochTime, job.scheduleTime)
		}
	}
}

func completeCollectionIndex(sIdx int, schedule *[]scheduleJob, dbMeta dbMetaObj) {
	// ASSERT: sched.Lock() is in place - by caller
	completeTime := time.Now().UnixNano()
	job := (*schedule)[sIdx]
	fileCount := len(job.files)
	scheduleId, startTime := updateJobAsComplete(dbMeta, job.collectionId, completeTime)
	(*schedule)[sIdx] = scheduleJob{}
	inFlight--
	log.Printf("Collection %q [id %d] completed %d files in %s - in-flight: %d", job.collectionId, scheduleId, fileCount, time.Duration(completeTime-startTime)*time.Nanosecond, inFlight)
}

func loadSchedule(dbMeta dbMetaObj, schedule *[]scheduleJob, zebedeeRoot string) {
	rows, err := dbMeta.prepped["load"].Query()
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			collectionId, encryptionKey           sql.NullString
			scheduleTime, startTime, completeTime sql.NullInt64
			scheduleId                            int64
		)

		if err = rows.Scan(&scheduleId, &collectionId, &scheduleTime, &encryptionKey, &startTime, &completeTime); err != nil {
			log.Fatal(err)
		}

		var files []string
		if files, err = findCollectionFiles(zebedeeRoot, collectionId.String); err != nil {
			log.Panic(err)
		}
		job := scheduleJob{scheduleId: scheduleId, scheduleTime: scheduleTime.Int64, collectionId: collectionId.String, encryptionKey: encryptionKey.String, filesToDo: len(files)}
		job.files = make([]string, len(files))
		copy(job.files, files)

		*schedule = append(*schedule, job)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d jobs", len(*schedule))
}

func storeJob(dbMeta dbMetaObj, job scheduleJob) int64 {
	res := dbMeta.prepped["store"].QueryRow(job.collectionId, job.scheduleTime, job.encryptionKey)
	var scheduleId int64
	if err := res.Scan(&scheduleId); err != nil {
		log.Panic(err)
	}
	return scheduleId
}

func updateJobAsStarted(dbMeta dbMetaObj, scheduleId int64, collectionId string, startTime int64) {
	_, err := dbMeta.prepped["update-publish"].Exec(collectionId, startTime, scheduleId)
	if err != nil {
		log.Panic(err)
	}
}

func updateJobAsComplete(dbMeta dbMetaObj, collectionId string, completeTime int64) (int64, int64) {
	var (
		startTime  sql.NullInt64
		scheduleId int64
	)
	res := dbMeta.prepped["update-complete"].QueryRow(collectionId, completeTime)
	if err := res.Scan(&scheduleId, &startTime); err != nil {
		log.Panic(err)
	}
	return scheduleId, startTime.Int64
}

func updateJobFileComplete(jsonMessage []byte, schedule *[]scheduleJob, dbMeta dbMetaObj) {
	var file kafka.FileCompleteFlagMessage
	if err := json.Unmarshal(jsonMessage, &file); err != nil {
		log.Printf("Failed to parse json message")
		return
	}
	if file.CollectionId == "" || file.FileLocation == "" {
		log.Printf("Json message is missing fields : %s", string(jsonMessage))
		return
	}

	sched.Lock()
	defer sched.Unlock()

	jobFound := false
	for i := 0; i < len(*schedule); i++ {
		if (*schedule)[i].collectionId == file.CollectionId {
			(*schedule)[i].filesToDo--
			if (*schedule)[i].filesToDo == 0 {
				completeCollectionIndex(i, schedule, dbMeta)
			} else if (*schedule)[i].filesToDo%250 == 0 {
				log.Printf("Collection %q has %d files to do", file.CollectionId, (*schedule)[i].filesToDo)
			}
			jobFound = true
			break
		}
	}
	if !jobFound {
		log.Printf("ERROR: Failed to find job %q in schedule for file %q", file.CollectionId, file.FileLocation)
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
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory %q", zebedeeRoot)
	}

	scheduleTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	completeFileTopic := utils.GetEnvironmentVariable("COMPLETE_FILE_FLAG_TOPIC", "uk.gov.ons.dp.web.complete-file-flag")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")

	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}
	if err = db.Ping(); err != nil {
		log.Panicf("Error: Could not establish a connection with the database: %s", err.Error())
	}
	dbMeta := dbMetaObj{db: db, prepped: make(map[string]*sql.Stmt)}
	dbMeta.prep("load", "SELECT schedule_id, collection_id, schedule_time, encryption_key, start_time, complete_time FROM schedule WHERE start_time IS NULL OR complete_time IS NULL ORDER BY schedule_time")
	dbMeta.prep("store", "INSERT INTO schedule (collection_id, schedule_time, encryption_key, start_time, complete_time) VALUES ($1, $2, $3, NULL, NULL) RETURNING schedule_id")
	dbMeta.prep("update-complete", "UPDATE schedule SET complete_time=$2 WHERE collection_id=$1 AND start_time IS NOT NULL AND complete_time IS NULL RETURNING schedule_id, start_time")
	dbMeta.prep("update-publish", "UPDATE schedule SET start_time=$2 WHERE schedule_id=$3 AND collection_id=$1 AND complete_time IS NULL")

	schedule := make([]scheduleJob, 0, 10)

	loadSchedule(dbMeta, &schedule, zebedeeRoot)

	log.Printf("Starting publish scheduler from %q topics: %q -> %q/%q", zebedeeRoot, scheduleTopic, produceFileTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	scheduleConsumer := kafka.NewConsumerGroup(scheduleTopic, "publish-scheduler")
	fileProducer := kafka.NewProducer(produceFileTopic)
	fileConsumer := kafka.NewConsumerGroup(completeFileTopic, "publish-scheduler")

	publishChannel := make(chan scheduleJob)

	for {
		select {
		case scheduleMessage := <-scheduleConsumer.Incoming:
			go func() {
				scheduleCollection(scheduleMessage.GetData(), &schedule, zebedeeRoot, dbMeta)
				scheduleMessage.Commit()
			}()
		case publishMessage := <-publishChannel:
			go publishCollection(publishMessage, fileProducer.Output, totalProducer.Output)
		case fileCompleteMessage := <-fileConsumer.Incoming:
			go func() {
				updateJobFileComplete(fileCompleteMessage.GetData(), &schedule, dbMeta)
				fileCompleteMessage.Commit()
			}()
		case <-time.After(tick):
			go checkSchedule(&schedule, publishChannel, dbMeta)
		}
	}
}
